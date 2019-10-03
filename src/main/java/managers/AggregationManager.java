package managers;

import beans.*;
import controllers.CacheSystemController;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class AggregationManager {
    CacheSystemController c;

    public AggregationManager(CacheSystemController c) {
        this.c = c;
    }

    public Dataset<Row> aggregateFromSL(Dataset<Row> rows, GranularityBean requiredGranularity, SLCacheTableBean sl) {
        c.logManager.logPriorityInfo("[aggregateFromSL]");
        Column window = window(col(sl.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = sum("count_agg_rows").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sl.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column power = sum("sum_power").as("sum_power");
            Column voltage = sum("sum_voltage").as("sum_voltage");
            Column current = sum("sum_current").as("sum_current");
            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            Column slotEnergyConsumed = sum("slot_energy_consumed").as("slot_energy_consumed");
            Column minPower = min(abs(col("min_power"))).as("min_power");
            Column minVoltage = min(abs(col("min_voltage"))).as("min_voltage");
            Column minCurrent = min(abs(col("min_current"))).as("min_current");
            Column maxPower = max(abs(col("max_power"))).as("max_power");
            Column maxVoltage = max(abs(col("max_voltage"))).as("max_voltage");
            Column maxCurrent = max(abs(col("max_current"))).as("max_current");

            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(power, voltage, current, energyConsumed, minPower, maxPower, minVoltage, maxVoltage, minCurrent, maxCurrent, slotEnergyConsumed, count_agg_rows);
        } else if (sl.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("sum_temperature").as("sum_temperature");
            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(temperature, count_agg_rows);
        } else if (sl.getSchemaType() == ConfigurationBean.SchemaType.temperature_humidity) {
            Column temperature = sum("sum_temperature").as("sum_temperature");
            Column humidity = sum("sum_humidity").as("sum_humidity");
            Column batteryVoltage = sum("sum_battery_voltage").as("sum_battery_voltage");
            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(temperature, humidity, batteryVoltage, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sl.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;
    }

    public Dataset<Row> aggregateFromSource(Dataset<Row> rows, GranularityBean requiredGranularity, SourceTableBean sourceTable) {
        c.logManager.logPriorityInfo("[aggregateFromSource]");
        Column window = window(col(sourceTable.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = count("*").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column power = sum(abs(col("power"))).as("sum_power");
            Column voltage = sum("voltage").as("sum_voltage");
            Column current = sum("current").as("sum_current");
            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            Column slotEnergyConsumed = last("energy_consumed").minus(first("energy_consumed")).as("slot_energy_consumed");
            Column minPower = min(abs(col("power"))).as("min_power");
            Column minVoltage = min(abs(col("voltage"))).as("min_voltage");
            Column minCurrent = min(abs(col("current"))).as("min_current");
            Column maxPower = max(abs(col("power"))).as("max_power");
            Column maxVoltage = max(abs(col("voltage"))).as("max_voltage");
            Column maxCurrent = max(abs(col("current"))).as("max_current");
            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(power, voltage, current, energyConsumed, minPower, maxPower, minVoltage, maxVoltage, minCurrent, maxCurrent, slotEnergyConsumed, count_agg_rows);
        } else if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("temperature").as("sum_temperature");
            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(temperature, count_agg_rows);
        } else if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.temperature_humidity) {
            Column temperature = sum("temperature").as("sum_temperature");
            Column humidity = sum("humidity").as("sum_humidity");
            Column batteryVoltage = sum("battery_voltage").as("sum_battery_voltage");
            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(temperature, humidity, batteryVoltage, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sourceTable.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;

    }

    public Dataset<Row> aggregateSpatially(Dataset<Row> rows, SensorGroupBean sensorGroupBean, SourceTableBean commonSourceTableSchema) {
        c.logManager.logPriorityInfo("[aggregateSpatially]");
        if (sensorGroupBean.getSpatialAggFunction().equalsIgnoreCase("sum")) {
            Dataset<Row> agg = null;
            if (commonSourceTableSchema.getSchemaType() == ConfigurationBean.SchemaType.power) {
                Column power = sum("power").as("power");
                Column voltage = sum("voltage").as("voltage");
                Column current = sum("current").as("current");
                Column energyConsumed = sum("energy_consumed").as("energy_consumed");
                agg = rows.groupBy(floor(col(commonSourceTableSchema.getTsColumnName())).as(commonSourceTableSchema.getTsColumnName()))
                        .agg(power, voltage, current, energyConsumed);
            } else if (commonSourceTableSchema.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
                Column temperature = sum("temperature").as("temperature");
                agg = rows.groupBy(floor(col(commonSourceTableSchema.getTsColumnName())).as(commonSourceTableSchema.getTsColumnName()))
                        .agg(temperature);
            } else if (commonSourceTableSchema.getSchemaType() == ConfigurationBean.SchemaType.temperature_humidity) {
                Column temperature = sum("temperature").as("temperature");
                Column humidity = sum("humidity").as("humidity");
                Column batteryVoltage = sum("battery_voltage").as("battery_voltage");
                agg = rows.groupBy(floor(col(commonSourceTableSchema.getTsColumnName())).as(commonSourceTableSchema.getTsColumnName()))
                        .agg(temperature, humidity, batteryVoltage);
            }
            assert agg != null;
            agg = agg.withColumn(commonSourceTableSchema.getSensorIdColumnName(), lit(sensorGroupBean.getSensorId()));
            return agg;
        }
        assert false;
        return null;
    }
}
