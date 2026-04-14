# 计划：Spark variant_get 列式投影下推到 Paimon Shredded Variant

## Context

Paimon 在 shredded 模式下存储 Variant 列时，format 层已有完整的列裁剪基础设施
（`VariantRowType` + `clipVariantType` + `assembleVariantStructBatch`），
但 Spark 侧没有把 `variant_get(v, '$.age', 'int')` 下推的逻辑，导致：
- Paimon 必须读所有 shredded 子列并全量 rebuild binary variant
- Spark 收到完整 VariantVal 后再自行提取字段
- IO 收益为零，甚至负收益

目标：新增 Catalyst Optimizer Rule，让 Spark 4 中的 `variant_get` 调用能触发
Paimon 的 `VariantRowType` 路径，只读需要的 `typed_value.xxx` 子列。

---

## 关键文件路径

| 角色 | 文件 |
|---|---|
| 新建 Rule（Spark 4） | `paimon-spark/paimon-spark4-common/src/main/scala/org/apache/paimon/spark/catalyst/optimizer/PushDownVariantExtract.scala` |
| PaimonScan（case class） | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonScan.scala` |
| BaseScan（trait） | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/read/BaseScan.scala` |
| Rule 注册 | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/extensions/PaimonSparkSessionExtensions.scala` |
| Shim 接口 | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/spark/sql/paimon/shims/SparkShim.scala` |
| Spark4Shim | `paimon-spark/paimon-spark4-common/src/main/scala/org/apache/spark/sql/paimon/shims/Spark4Shim.scala` |
| Spark3Shim | `paimon-spark/paimon-spark3-common/src/main/scala/org/apache/spark/sql/paimon/shims/Spark3Shim.scala` |
| MergePaimonScalarSubqueriesBase | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/catalyst/optimizer/MergePaimonScalarSubqueriesBase.scala` |
| VariantRowTypeBuilder | `paimon-common/src/main/java/org/apache/paimon/data/variant/VariantMetadataUtils.java` |
| SparkTypeUtils | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkTypeUtils.java` |

---

## 实现步骤

### Step 1：`PaimonScan.scala` — 添加 `variantProjections` 字段

在 case class 末尾追加一个带默认值的参数，同时覆盖 BaseScan 的钩子方法：

```scala
case class PaimonScan(
    table: InnerTable,
    requiredSchema: StructType,
    ...,                              // 已有参数不变
    bucketedScanDisabled: Boolean = false,
    variantProjections: Map[String, RowType] = Map.empty   // 新增
) extends PaimonBaseScan(table) ... {

  // 覆盖 BaseScan 的钩子，供 readTableRowType 使用
  override protected def variantProjectionMap: Map[String, RowType] = variantProjections
  
  // 已有方法不变...
}
```

---

### Step 2：`BaseScan.scala` — 在 `readTableRowType` 中应用 variant 投影

添加钩子 def 和辅助方法，修改 `readTableRowType` 的计算：

```scala
trait BaseScan extends Scan ... {

  // 钩子，子类可以覆盖（PaimonScan 会覆盖）
  protected def variantProjectionMap: Map[String, RowType] = Map.empty

  private[paimon] val (readTableRowType, metadataFields) = {
    requiredSchema.fields.foreach(f => checkMetadataColumn(f.name))
    val (_requiredTableFields, _metadataFields) =
      requiredSchema.fields.partition(field => tableRowType.containsField(field.name))
    val _readTableRowType =
      SparkTypeUtils.prunePaimonRowType(StructType(_requiredTableFields), tableRowType)
    
    // 新增：将 VariantType 字段替换为 VariantRowType
    val _finalReadType = applyVariantProjections(_readTableRowType, variantProjectionMap)
    (_finalReadType, _metadataFields)
  }

  // 新增辅助方法
  private def applyVariantProjections(
      rowType: RowType,
      projections: Map[String, RowType]): RowType = {
    if (projections.isEmpty) return rowType
    val newFields = rowType.getFields.asScala.map { field =>
      projections.get(field.name()) match {
        case Some(variantRowType) => field.newType(variantRowType)
        case None                 => field
      }
    }
    rowType.copy(newFields.asJava)
  }
  
  // readSchema() 无需改动：fromPaimonRowType(VariantRowType) 自动返回 StructType
  // （因为 VariantRowType 是 RowType，PaimonToSparkTypeVisitor 的 RowType visit 返回 StructType）
}
```

> **初始化顺序说明**：Scala trait 的 `val` 初始化是 mixin 到具体类的 `<init>` 中，
> 在 case class 的参数字段赋值之后执行，因此 `variantProjectionMap` 访问
> `variantProjections` 参数字段时该字段已经初始化。与现有 `requiredSchema` 的用法一致。

---

### Step 3：新建 `PushDownVariantExtract.scala`（Spark 4 only）

```scala
package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.data.variant.VariantMetadataUtils
import org.apache.paimon.spark.{PaimonScan, SparkTypeUtils}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.VariantType

import scala.collection.mutable
import scala.collection.JavaConverters._

object PushDownVariantExtract extends Rule[LogicalPlan] {

  // 描述一个 VariantGet 的唯一标识（用于去重）
  private case class GetKey(path: String, targetType: DataType,
                             failOnError: Boolean, timeZoneId: String)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case project @ Project(projectList, rel: DataSourceV2ScanRelation)
        if rel.scan.isInstanceOf[PaimonScan] =>

      val scan = rel.scan.asInstanceOf[PaimonScan]

      // 找出输出中所有 VariantType 属性（按名字索引）
      val variantAttrByName = rel.output
        .filter(_.dataType == VariantType)
        .map(a => a.name -> a).toMap
      if (variantAttrByName.isEmpty) return project

      // 遍历 project 表达式，收集 VariantGet 和直接引用
      val variantGetsByCol = mutable.Map[String, mutable.ListBuffer[VariantGet]]()
      val colUsedDirectly = mutable.Set[String]()

      def collect(expr: Expression): Unit = expr match {
        case vg: VariantGet if vg.path.isInstanceOf[Literal] =>
          vg.child match {
            case ar: AttributeReference if variantAttrByName.contains(ar.name) =>
              variantGetsByCol.getOrElseUpdate(ar.name, mutable.ListBuffer.empty) += vg
            case _ => // nested VariantGet，不处理
          }
        // 不递归进 VariantGet 内部（child 已被上面匹配）
        case ar: AttributeReference if variantAttrByName.contains(ar.name) =>
          colUsedDirectly += ar.name
        case other =>
          other.children.foreach(collect)
      }
      projectList.foreach(collect)

      // 只下推「仅通过 VariantGet 使用」的列
      val pushableCols = variantGetsByCol.keys.filterNot(colUsedDirectly.contains).toSet
      if (pushableCols.isEmpty) return project

      // 为每列构建 VariantRowType，同时记录 GetKey -> field 序号的映射
      val newVariantRowTypes = mutable.Map[String, org.apache.paimon.types.RowType]()
      val getKeyToIndex = mutable.Map[(String, GetKey), Int]()

      pushableCols.foreach { colName =>
        val builder = VariantMetadataUtils.VariantRowTypeBuilder.builder()
        val seen = mutable.LinkedHashMap[GetKey, Int]()

        variantGetsByCol(colName).foreach { vg =>
          val path = vg.path.asInstanceOf[Literal].value.toString
          val tz   = vg.timeZoneId.getOrElse("UTC")
          val key  = GetKey(path, vg.targetType, vg.failOnError, tz)
          if (!seen.contains(key)) {
            val idx = seen.size
            seen(key) = idx
            val paimonType = SparkTypeUtils.toPaimonType(vg.targetType)
            builder.field(paimonType, path, vg.failOnError, tz)
          }
          getKeyToIndex((colName, key)) = seen(key)
        }
        newVariantRowTypes(colName) = builder.build()
      }

      // 创建新 scan（带 variantProjections）
      val newScan = scan.copy(variantProjections = newVariantRowTypes.toMap)

      // 更新 output：variant 列的 dataType 从 VariantType → StructType
      // fromPaimonRowType(VariantRowType) 自动产生 StructType（字段名为 "0","1"...）
      val newOutput = rel.output.map { attr =>
        newVariantRowTypes.get(attr.name) match {
          case Some(vrt) => attr.copy(dataType = SparkTypeUtils.fromPaimonRowType(vrt))
          case None      => attr
        }
      }
      val newRel = rel.copy(scan = newScan, output = newOutput)
      val newAttrByName = newOutput.map(a => a.name -> a).toMap

      // 重写 projectList：VariantGet → GetStructField(structAttr, ordinal)
      val newProjectList = projectList.map(_.transform {
        case vg: VariantGet if vg.path.isInstanceOf[Literal] =>
          vg.child match {
            case ar: AttributeReference if newVariantRowTypes.contains(ar.name) =>
              val path = vg.path.asInstanceOf[Literal].value.toString
              val tz   = vg.timeZoneId.getOrElse("UTC")
              val key  = GetKey(path, vg.targetType, vg.failOnError, tz)
              val idx  = getKeyToIndex((ar.name, key))
              GetStructField(newAttrByName(ar.name), idx)
            case _ => vg
          }
      }).asInstanceOf[Seq[NamedExpression]]

      Project(newProjectList, newRel)
  }
}
```

---

### Step 4：Shim 接口与实现（Spark 版本隔离）

**SparkShim.scala**（添加接口方法）：
```scala
// 返回 Spark 4 only 的 PushDownVariantExtract rule；Spark 3 返回 None
def variantExtractRule(): Option[Rule[LogicalPlan]] = None
```

**Spark4Shim.scala**（实现）：
```scala
override def variantExtractRule(): Option[Rule[LogicalPlan]] =
  Some(PushDownVariantExtract)
```

**Spark3Shim.scala**（默认 None，不用实现，继承 trait 默认值即可）

---

### Step 5：`PaimonSparkSessionExtensions.scala` — 注册 Rule

```scala
// 新增（通过 shim 注册，Spark 3 自动 skip）
SparkShimLoader.shim().variantExtractRule().foreach { rule =>
  extensions.injectOptimizerRule(_ => rule)
}
```

---

### Step 6：`MergePaimonScalarSubqueriesBase.scala` — 兼容 variantProjections

```scala
protected def mergePaimonScan(scan1: PaimonScan, scan2: PaimonScan): Option[PaimonScan] = {
  if (scan1 == scan2) {
    Some(scan2)
  } else if (scan1 == scan2.copy(
      requiredSchema = scan1.requiredSchema,
      variantProjections = scan1.variantProjections)) {   // 新增条件
    // 两个 scan 除 requiredSchema 和 variantProjections 外完全相同
    // 只有 variantProjections 相同时才合并（否则语义不一致）
    if (scan1.variantProjections != scan2.variantProjections) return None
    val mergedSchema = StructType(
      (scan2.requiredSchema.fields.toSet ++ scan1.requiredSchema.fields.toSet).toArray)
    Some(scan2.copy(requiredSchema = mergedSchema))
  } else {
    None
  }
}
```

---

## 数据流（实现后）

```
Spark SQL: SELECT variant_get(v, '$.age', 'int') FROM t
    ↓ PushDownVariantExtract.apply()
    ↓ 检测到 VariantGet(v, '$.age', IntegerType) 且 v 仅通过 VariantGet 使用
    ↓ 构建 VariantRowType { 0: INT, description="__VARIANT_METADATA$.age;true;UTC" }
    ↓ 创建新 PaimonScan(variantProjections = Map("v" -> VariantRowType))
    ↓ DataSourceV2ScanRelation.output: v#1: StructType<0: IntegerType>  (非 VariantType)
    ↓ Project: GetStructField(v#1, 0) 替换 VariantGet(v#1, ...)

BaseScan.readTableRowType:
    ↓ applyVariantProjections(): v 的字段类型替换为 VariantRowType
    ↓ table.newReadBuilder().withReadType(RowType { v: VariantRowType })

Paimon format 层（已有逻辑）：
    ↓ FormatReaderMapping.pruneDataType(): VariantRowType 直接透传
    ↓ clipVariantType(): 只保留 typed_value.age 子列 + metadata（跳过 value overflow）
    ↓ assembleVariantStructBatch(): 从 typed_value.age 直接读 INT，输出 struct{0: 35}

Spark 执行：
    ↓ GetStructField(struct{0: 35}, 0) = 35
```

---

## 注意事项

1. **`SparkTypeUtils.toPaimonType(DataType)`**：需确认此静态方法存在（或用等价的 visitor 调用）。
   如不存在，需在 SparkTypeUtils 中添加一个公共静态方法。

2. **非字面量 path**：rule 中已加 `vg.path.isInstanceOf[Literal]` 守卫，非字面量路径不下推。

3. **非 shredded 文件**：`assembleVariantStructBatch` 内部对非 shredded 文件有 fallback（读 `value` binary 后 cast），行为正确，只是没有 IO 收益。

4. **Filter 中的 variant_get**：本次只处理 Project，Filter 中的下推留给后续。

---

## 验证方式

1. **单元测试**：新建 `PushDownVariantExtractTest.scala`，验证 rule 前后逻辑计划的变化：
   - `variant_get(v, '$.age', 'int')` → `GetStructField`
   - `v` 直接引用时不下推
   - `try_variant_get` 同样处理（failOnError=false）

2. **集成测试**：在现有 `VariantShreddingReadTest` 或新建 Spark IT 中，写入 shredded 表，
   执行 `SELECT variant_get(v, '$.age', 'int') FROM t`，
   用 `df.queryExecution.executedPlan.toString` 验证 scan 的 `readSchema` 包含 StructType 而非 VariantType。

3. **IO 验证**：通过 `TaskMetrics.inputMetrics.bytesRead` 对比 shredded 场景下有无该 rule 时的字节数差异。

