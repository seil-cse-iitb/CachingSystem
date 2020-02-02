package managers;

import beans.*;
import controllers.CacheSystemController;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/*
* For Aggregation:
*
* for Normal & Spatial aggregation
*
* Power:
voltage_1:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
voltage_2:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
voltage_3:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
current_1:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
current_2:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
current_3:       Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_1:         Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_2:         Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_3:         Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_factor_1:  Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_factor_2:  Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
power_factor_3:  Take ABSOLUTE then SUM (finally show AVERAGE using the sum), Also take MIN & MAX to find anomaly
energy_consumed: Take LAST
* */

public class AggregationManager {
    CacheSystemController c;

    public AggregationManager(CacheSystemController c) {
        this.c = c;
    }

    public Dataset<Row> aggregateFromSL(Dataset<Row> rows, GranularityBean requiredGranularity, SLCacheTableBean sl) {
        LogManager.logPriorityInfo("[aggregateFromSL]");
        Column window = window(col(sl.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = sum("count_agg_rows").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sl.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column voltage_1 = sum(abs(col("sum_voltage_1"))).as("sum_voltage_1");
            Column min_voltage_1 = min(abs(col("min_voltage_1"))).as("min_voltage_1");
            Column max_voltage_1 = max(abs(col("max_voltage_1"))).as("max_voltage_1");

            Column voltage_2 = sum(abs(col("sum_voltage_2"))).as("sum_voltage_2");
            Column min_voltage_2 = min(abs(col("min_voltage_2"))).as("min_voltage_2");
            Column max_voltage_2 = max(abs(col("max_voltage_2"))).as("max_voltage_2");

            Column voltage_3 = sum(abs(col("sum_voltage_3"))).as("sum_voltage_3");
            Column min_voltage_3 = min(abs(col("min_voltage_3"))).as("min_voltage_3");
            Column max_voltage_3 = max(abs(col("max_voltage_3"))).as("max_voltage_3");

            Column current_1 = sum(abs(col("sum_current_1"))).as("sum_current_1");
            Column min_current_1 = min(abs(col("min_current_1"))).as("min_current_1");
            Column max_current_1 = max(abs(col("max_current_1"))).as("max_current_1");

            Column current_2 = sum(abs(col("sum_current_2"))).as("sum_current_2");
            Column min_current_2 = min(abs(col("min_current_2"))).as("min_current_2");
            Column max_current_2 = max(abs(col("max_current_2"))).as("max_current_2");

            Column current_3 = sum(abs(col("sum_current_3"))).as("sum_current_3");
            Column min_current_3 = min(abs(col("min_current_3"))).as("min_current_3");
            Column max_current_3 = max(abs(col("max_current_3"))).as("max_current_3");

            Column power_1 = sum(abs(col("sum_power_1"))).as("sum_power_1");
            Column min_power_1 = min(abs(col("min_power_1"))).as("min_power_1");
            Column max_power_1 = max(abs(col("max_power_1"))).as("max_power_1");

            Column power_2 = sum(abs(col("sum_power_2"))).as("sum_power_2");
            Column min_power_2 = min(abs(col("min_power_2"))).as("min_power_2");
            Column max_power_2 = max(abs(col("max_power_2"))).as("max_power_2");

            Column power_3 = sum(abs(col("sum_power_3"))).as("sum_power_3");
            Column min_power_3 = min(abs(col("min_power_3"))).as("min_power_3");
            Column max_power_3 = max(abs(col("max_power_3"))).as("max_power_3");

            Column power_factor_1 = sum(abs(col("sum_power_factor_1"))).as("sum_power_factor_1");
            Column min_power_factor_1 = min(abs(col("min_power_factor_1"))).as("min_power_factor_1");
            Column max_power_factor_1 = max(abs(col("max_power_factor_1"))).as("max_power_factor_1");

            Column power_factor_2 = sum(abs(col("sum_power_factor_2"))).as("sum_power_factor_2");
            Column min_power_factor_2 = min(abs(col("min_power_factor_2"))).as("min_power_factor_2");
            Column max_power_factor_2 = max(abs(col("max_power_factor_2"))).as("max_power_factor_2");

            Column power_factor_3 = sum(abs(col("sum_power_factor_3"))).as("sum_power_factor_3");
            Column min_power_factor_3 = min(abs(col("min_power_factor_3"))).as("min_power_factor_3");
            Column max_power_factor_3 = max(abs(col("max_power_factor_3"))).as("max_power_factor_3");

            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            Column slotEnergyConsumed = sum("slot_energy_consumed").as("slot_energy_consumed");

            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(voltage_1, min_voltage_1, max_voltage_1, voltage_2, min_voltage_2, max_voltage_2, voltage_3, min_voltage_3, max_voltage_3, current_1, min_current_1, max_current_1, current_2, min_current_2, max_current_2, current_3, min_current_3, max_current_3, power_1, min_power_1, max_power_1, power_2, min_power_2, max_power_2, power_3, min_power_3, max_power_3, power_factor_1, min_power_factor_1, max_power_factor_1, power_factor_2, min_power_factor_2, max_power_factor_2, power_factor_3, min_power_factor_3, max_power_factor_3, energyConsumed, slotEnergyConsumed, count_agg_rows);
        } else if (sl.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("sum_temperature").as("sum_temperature");
            Column min_temperature = min("min_temperature").as("min_temperature");
            Column max_temperature = max("max_temperature").as("max_temperature");

            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(temperature, min_temperature, max_temperature, count_agg_rows);
        } else if (sl.getSchemaType() == ConfigurationBean.SchemaType.temperature_humidity) {
            Column temperature = sum("sum_temperature").as("sum_temperature");
            Column min_temperature = min("min_temperature").as("min_temperature");
            Column max_temperature = max("max_temperature").as("max_temperature");

            Column humidity = sum("sum_humidity").as("sum_humidity");
            Column min_humidity = min("min_humidity").as("min_humidity");
            Column max_humidity = max("max_humidity").as("max_humidity");

            Column batteryVoltage = sum("sum_battery_voltage").as("sum_battery_voltage");
            Column min_battery_voltage = min("min_battery_voltage").as("min_battery_voltage");
            Column max_battery_voltage = max("max_battery_voltage").as("max_battery_voltage");

            agg = rows.groupBy(window, col(sl.getSensorIdColumnName()))
                    .agg(temperature, min_temperature, max_temperature, humidity, min_humidity, max_humidity, batteryVoltage, min_battery_voltage, max_battery_voltage, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sl.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;
    }

    public Dataset<Row> aggregateFromSource(Dataset<Row> rows, GranularityBean requiredGranularity, SourceTableBean sourceTable) {
        LogManager.logPriorityInfo("[aggregateFromSource]");
        Column window = window(col(sourceTable.getTsColumnName()).cast(DataTypes.TimestampType), requiredGranularity.getWindowDuration());
        Column count_agg_rows = count("*").as("count_agg_rows");
        Dataset<Row> agg = null;
        if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.power) {
            Column voltage_1 = sum(abs(col("voltage_1"))).as("sum_voltage_1");
            Column min_voltage_1 = min(abs(col("voltage_1"))).as("min_voltage_1");
            Column max_voltage_1 = max(abs(col("voltage_1"))).as("max_voltage_1");

            Column voltage_2 = sum(abs(col("voltage_2"))).as("sum_voltage_2");
            Column min_voltage_2 = min(abs(col("voltage_2"))).as("min_voltage_2");
            Column max_voltage_2 = max(abs(col("voltage_2"))).as("max_voltage_2");

            Column voltage_3 = sum(abs(col("voltage_3"))).as("sum_voltage_3");
            Column min_voltage_3 = min(abs(col("voltage_3"))).as("min_voltage_3");
            Column max_voltage_3 = max(abs(col("voltage_3"))).as("max_voltage_3");

            Column current_1 = sum(abs(col("current_1"))).as("sum_current_1");
            Column min_current_1 = min(abs(col("current_1"))).as("min_current_1");
            Column max_current_1 = max(abs(col("current_1"))).as("max_current_1");

            Column current_2 = sum(abs(col("current_2"))).as("sum_current_2");
            Column min_current_2 = min(abs(col("current_2"))).as("min_current_2");
            Column max_current_2 = max(abs(col("current_2"))).as("max_current_2");

            Column current_3 = sum(abs(col("current_3"))).as("sum_current_3");
            Column min_current_3 = min(abs(col("current_3"))).as("min_current_3");
            Column max_current_3 = max(abs(col("current_3"))).as("max_current_3");

            Column power_1 = sum(abs(col("power_1"))).as("sum_power_1");
            Column min_power_1 = min(abs(col("power_1"))).as("min_power_1");
            Column max_power_1 = max(abs(col("power_1"))).as("max_power_1");

            Column power_2 = sum(abs(col("power_2"))).as("sum_power_2");
            Column min_power_2 = min(abs(col("power_2"))).as("min_power_2");
            Column max_power_2 = max(abs(col("power_2"))).as("max_power_2");

            Column power_3 = sum(abs(col("power_3"))).as("sum_power_3");
            Column min_power_3 = min(abs(col("power_3"))).as("min_power_3");
            Column max_power_3 = max(abs(col("power_3"))).as("max_power_3");

            Column power_factor_1 = sum(abs(col("power_factor_1"))).as("sum_power_factor_1");
            Column min_power_factor_1 = min(abs(col("power_factor_1"))).as("min_power_factor_1");
            Column max_power_factor_1 = max(abs(col("power_factor_1"))).as("max_power_factor_1");

            Column power_factor_2 = sum(abs(col("power_factor_2"))).as("sum_power_factor_2");
            Column min_power_factor_2 = min(abs(col("power_factor_2"))).as("min_power_factor_2");
            Column max_power_factor_2 = max(abs(col("power_factor_2"))).as("max_power_factor_2");

            Column power_factor_3 = sum(abs(col("power_factor_3"))).as("sum_power_factor_3");
            Column min_power_factor_3 = min(abs(col("power_factor_3"))).as("min_power_factor_3");
            Column max_power_factor_3 = max(abs(col("power_factor_3"))).as("max_power_factor_3");

            Column energyConsumed = last("energy_consumed").as("energy_consumed");
            Column slotEnergyConsumed = last("energy_consumed").minus(first("energy_consumed")).as("slot_energy_consumed");

            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(voltage_1, min_voltage_1, max_voltage_1, voltage_2, min_voltage_2, max_voltage_2, voltage_3, min_voltage_3, max_voltage_3, current_1, min_current_1, max_current_1, current_2, min_current_2, max_current_2, current_3, min_current_3, max_current_3, power_1, min_power_1, max_power_1, power_2, min_power_2, max_power_2, power_3, min_power_3, max_power_3, power_factor_1, min_power_factor_1, max_power_factor_1, power_factor_2, min_power_factor_2, max_power_factor_2, power_factor_3, min_power_factor_3, max_power_factor_3, energyConsumed, slotEnergyConsumed, count_agg_rows);
        } else if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.temperature) {
            Column temperature = sum("temperature").as("sum_temperature");
            Column min_temperature = min("temperature").as("min_temperature");
            Column max_temperature = max("temperature").as("max_temperature");

            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(temperature, min_temperature, max_temperature, count_agg_rows);
        } else if (sourceTable.getSchemaType() == ConfigurationBean.SchemaType.temperature_humidity) {
            Column temperature = sum("temperature").as("sum_temperature");
            Column min_temperature = min("temperature").as("min_temperature");
            Column max_temperature = max("temperature").as("max_temperature");

            Column humidity = sum("humidity").as("sum_humidity");
            Column min_humidity = min("humidity").as("min_humidity");
            Column max_humidity = max("humidity").as("max_humidity");

            Column batteryVoltage = sum("battery_voltage").as("sum_battery_voltage");
            Column min_battery_voltage = min("battery_voltage").as("min_battery_voltage");
            Column max_battery_voltage = max("battery_voltage").as("max_battery_voltage");

            agg = rows.groupBy(window, col(sourceTable.getSensorIdColumnName()).as(sourceTable.getSensorIdColumnName()))
                    .agg(temperature, min_temperature, max_temperature, humidity, min_humidity, max_humidity, batteryVoltage, min_battery_voltage, max_battery_voltage, count_agg_rows);
        }
        assert agg != null;
        agg = agg.withColumn(sourceTable.getTsColumnName(), col("window.start").cast(DataTypes.DoubleType)).drop("window")
                .withColumn("granularityId", lit(requiredGranularity.getGranularityId()));
        return agg;

    }

    public Dataset<Row> aggregateSpatially(Dataset<Row> rows, SensorGroupBean sensorGroupBean, SourceTableBean commonSourceTableSchema) {
        LogManager.logPriorityInfo("[aggregateSpatially]");
        if (sensorGroupBean.getSpatialAggFunction().equalsIgnoreCase("sum")) {
            Dataset<Row> agg = null;
            if (commonSourceTableSchema.getSchemaType() == ConfigurationBean.SchemaType.power) {
                Column voltage_1 = sum(abs(col("voltage_1"))).as("voltage_1");
                Column voltage_2 = sum(abs(col("voltage_2"))).as("voltage_2");
                Column voltage_3 = sum(abs(col("voltage_3"))).as("voltage_3");
                Column current_1 = sum(abs(col("current_1"))).as("current_1");
                Column current_2 = sum(abs(col("current_2"))).as("current_2");
                Column current_3 = sum(abs(col("current_3"))).as("current_3");
                Column power_1 = sum(abs(col("power_1"))).as("power_1");
                Column power_2 = sum(abs(col("power_2"))).as("power_2");
                Column power_3 = sum(abs(col("power_3"))).as("power_3");
                Column power_factor_1 = sum(abs(col("power_factor_1"))).as("power_factor_1");
                Column power_factor_2 = sum(abs(col("power_factor_2"))).as("power_factor_2");
                Column power_factor_3 = sum(abs(col("power_factor_3"))).as("power_factor_3");
                Column energyConsumed = sum("energy_consumed").as("energy_consumed");
                agg = rows.groupBy(floor(col(commonSourceTableSchema.getTsColumnName())).as(commonSourceTableSchema.getTsColumnName()))
                        .agg(voltage_1, voltage_2, voltage_3, current_1, current_2, current_3, power_1, power_2, power_3, power_factor_1, power_factor_2, power_factor_3, energyConsumed);
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
