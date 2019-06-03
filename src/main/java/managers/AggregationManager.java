package managers;

import beans.*;
import controllers.CacheSystemController;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class AggregationManager {
    public AggregationManager(CacheSystemController cb) {
    }

    public Dataset<Row> aggregateFromSL(Dataset<Row> rows, GranularityBean requiredGranularity, SLCacheTableBean sl) {
        Column window = window(col(sl.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = sum("count_agg_rows").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sl.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column power = sum("sum_power").as("sum_power");
            Column voltage = sum("sum_voltage").as("sum_voltage");
            Column current = sum("sum_current").as("sum_current");
            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(power, voltage, current, energyConsumed, count_agg_rows);
        } else if (sl.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("sum_temperature").as("sum_temperature");
            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(temperature, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sl.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;
    }

    public Dataset<Row> aggregateFromSource(Dataset<Row> rows, GranularityBean requiredGranularity, SourceTableBean sourceTable) {
        Column window = window(col(sourceTable.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = count("*").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column power = sum("power").as("sum_power");
            Column voltage = sum("voltage").as("sum_voltage");
            Column current = avg("current").as("sum_current");
            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(power, voltage, current, energyConsumed, count_agg_rows);
        } else if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("temperature").as("sum_temperature");
            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(temperature, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sourceTable.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;

    }
}
