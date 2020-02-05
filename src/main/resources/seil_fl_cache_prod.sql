-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- Host: 10.129.149.22
-- Generation Time: Feb 05, 2020 at 05:52 PM
-- Server version: 5.7.26
-- PHP Version: 7.2.14

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `seil_fl_cache_prod`
--
CREATE DATABASE IF NOT EXISTS `seil_fl_cache_prod` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `seil_fl_cache_prod`;

-- --------------------------------------------------------

--
-- Table structure for table `granularity`
--

CREATE TABLE IF NOT EXISTS `granularity` (
  `displayLimitInSeconds` int(11) DEFAULT NULL,
  `displayPriority` int(11) DEFAULT NULL,
  `fetchIntervalAtOnceInSeconds` int(11) DEFAULT NULL,
  `granularityId` text,
  `granularityInTermsOfSeconds` int(11) DEFAULT NULL,
  `numParallelQuery` int(11) DEFAULT NULL,
  `numPartitionsForEachInterval` int(11) DEFAULT NULL,
  `windowDuration` text
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Stand-in structure for view `power`
-- (See below for the actual view)
--
CREATE TABLE IF NOT EXISTS `power` (
`energy_consumed` double
,`slot_energy_consumed` double
,`max_power` double
,`min_power` double
,`granularity` varchar(50)
,`sensor_id` varchar(50)
,`ts` double
,`power` double
,`voltage` double
,`current` double
,`agg_row_count` bigint(20)
);

-- --------------------------------------------------------

--
-- Table structure for table `power_cache`
--

CREATE TABLE IF NOT EXISTS `power_cache` (
  `sensor_id` varchar(50) DEFAULT NULL,
  `sum_voltage_1` double DEFAULT NULL,
  `min_voltage_1` double DEFAULT NULL,
  `max_voltage_1` double DEFAULT NULL,
  `sum_voltage_2` double DEFAULT NULL,
  `min_voltage_2` double DEFAULT NULL,
  `max_voltage_2` double DEFAULT NULL,
  `sum_voltage_3` double DEFAULT NULL,
  `min_voltage_3` double DEFAULT NULL,
  `max_voltage_3` double DEFAULT NULL,
  `sum_current_1` double DEFAULT NULL,
  `min_current_1` double DEFAULT NULL,
  `max_current_1` double DEFAULT NULL,
  `sum_current_2` double DEFAULT NULL,
  `min_current_2` double DEFAULT NULL,
  `max_current_2` double DEFAULT NULL,
  `sum_current_3` double DEFAULT NULL,
  `min_current_3` double DEFAULT NULL,
  `max_current_3` double DEFAULT NULL,
  `sum_power_1` double DEFAULT NULL,
  `min_power_1` double DEFAULT NULL,
  `max_power_1` double DEFAULT NULL,
  `sum_power_2` double DEFAULT NULL,
  `min_power_2` double DEFAULT NULL,
  `max_power_2` double DEFAULT NULL,
  `sum_power_3` double DEFAULT NULL,
  `min_power_3` double DEFAULT NULL,
  `max_power_3` double DEFAULT NULL,
  `sum_power_factor_1` double DEFAULT NULL,
  `min_power_factor_1` double DEFAULT NULL,
  `max_power_factor_1` double DEFAULT NULL,
  `sum_power_factor_2` double DEFAULT NULL,
  `min_power_factor_2` double DEFAULT NULL,
  `max_power_factor_2` double DEFAULT NULL,
  `sum_power_factor_3` double DEFAULT NULL,
  `min_power_factor_3` double DEFAULT NULL,
  `max_power_factor_3` double DEFAULT NULL,
  `energy_consumed` double DEFAULT NULL,
  `slot_energy_consumed` double DEFAULT NULL,
  `count_agg_rows` bigint(20) NOT NULL,
  `ts` double DEFAULT NULL,
  `granularityId` varchar(50) NOT NULL,
  KEY `sensor_id` (`sensor_id`,`ts`),
  KEY `granularityId` (`granularityId`,`sensor_id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `power_cache_bitmap`
--

CREATE TABLE IF NOT EXISTS `power_cache_bitmap` (
  `sensor_id` varchar(200) NOT NULL,
  `granularity` varchar(50) NOT NULL,
  `bitmapStartTime` mediumtext,
  `bitmapEndTime` mediumtext,
  `fl_bitset` mediumblob,
  `sl_bitset` mediumblob,
  PRIMARY KEY (`sensor_id`,`granularity`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Stand-in structure for view `temperature`
-- (See below for the actual view)
--
CREATE TABLE IF NOT EXISTS `temperature` (
`granularity` varchar(50)
,`sensor_id` varchar(50)
,`ts` double
,`temperature` double
,`min_temperature` double
,`max_temperature` double
);

-- --------------------------------------------------------

--
-- Table structure for table `temperature_cache`
--

CREATE TABLE IF NOT EXISTS `temperature_cache` (
  `sensor_id` varchar(50) DEFAULT NULL,
  `sum_temperature` double DEFAULT NULL,
  `min_temperature` double DEFAULT NULL,
  `max_temperature` double DEFAULT NULL,
  `count_agg_rows` bigint(20) NOT NULL,
  `ts` double DEFAULT NULL,
  `granularityId` varchar(50) NOT NULL,
  KEY `sensor_id` (`sensor_id`,`ts`),
  KEY `sensor_id_2` (`granularityId`,`sensor_id`,`ts`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `temperature_cache_bitmap`
--

CREATE TABLE IF NOT EXISTS `temperature_cache_bitmap` (
  `sensor_id` varchar(200) NOT NULL,
  `granularity` varchar(50) NOT NULL,
  `bitmapStartTime` mediumtext,
  `bitmapEndTime` mediumtext,
  `fl_bitset` mediumblob,
  `sl_bitset` mediumblob,
  PRIMARY KEY (`sensor_id`,`granularity`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Stand-in structure for view `temperature_humidity`
-- (See below for the actual view)
--
CREATE TABLE IF NOT EXISTS `temperature_humidity` (
`granularity` varchar(50)
,`sensor_id` varchar(50)
,`ts` double
,`temperature` double
,`humidity` double
,`sum_battery_voltage/ count_agg_rows` double
,`min_temperature` double
,`max_temperature` double
,`min_humidity` double
,`max_humidity` double
,`min_battery_voltage` double
,`max_battery_voltage` double
);

-- --------------------------------------------------------

--
-- Table structure for table `temperature_humidity_cache`
--

CREATE TABLE IF NOT EXISTS `temperature_humidity_cache` (
  `sensor_id` varchar(50) DEFAULT NULL,
  `sum_temperature` double DEFAULT NULL,
  `min_temperature` double DEFAULT NULL,
  `max_temperature` double DEFAULT NULL,
  `sum_humidity` double DEFAULT NULL,
  `min_humidity` double DEFAULT NULL,
  `max_humidity` double DEFAULT NULL,
  `sum_battery_voltage` double DEFAULT NULL,
  `min_battery_voltage` double DEFAULT NULL,
  `max_battery_voltage` double DEFAULT NULL,
  `count_agg_rows` bigint(20) NOT NULL,
  `ts` double DEFAULT NULL,
  `granularityId` varchar(50) NOT NULL,
  KEY `sensor_id` (`sensor_id`,`ts`),
  KEY `granularityId` (`granularityId`,`sensor_id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `temperature_humidity_cache_bitmap`
--

CREATE TABLE IF NOT EXISTS `temperature_humidity_cache_bitmap` (
  `sensor_id` varchar(200) NOT NULL,
  `granularity` varchar(50) NOT NULL,
  `bitmapStartTime` mediumtext,
  `bitmapEndTime` mediumtext,
  `fl_bitset` mediumblob,
  `sl_bitset` mediumblob,
  PRIMARY KEY (`sensor_id`,`granularity`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Structure for view `power`
--
DROP TABLE IF EXISTS `power`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `power`  AS  select `power_cache`.`energy_consumed` AS `energy_consumed`,`power_cache`.`slot_energy_consumed` AS `slot_energy_consumed`,`power_cache`.`max_power_1` AS `max_power`,`power_cache`.`min_power_1` AS `min_power`,`power_cache`.`granularityId` AS `granularity`,`power_cache`.`sensor_id` AS `sensor_id`,`power_cache`.`ts` AS `ts`,(`power_cache`.`sum_power_1` / `power_cache`.`count_agg_rows`) AS `power`,(`power_cache`.`sum_voltage_1` / `power_cache`.`count_agg_rows`) AS `voltage`,(`power_cache`.`sum_current_1` / `power_cache`.`count_agg_rows`) AS `current`,`power_cache`.`count_agg_rows` AS `agg_row_count` from `power_cache` ;

-- --------------------------------------------------------

--
-- Structure for view `temperature`
--
DROP TABLE IF EXISTS `temperature`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `temperature`  AS  select `temperature_cache`.`granularityId` AS `granularity`,`temperature_cache`.`sensor_id` AS `sensor_id`,`temperature_cache`.`ts` AS `ts`,(`temperature_cache`.`sum_temperature` / `temperature_cache`.`count_agg_rows`) AS `temperature`,`temperature_cache`.`min_temperature` AS `min_temperature`,`temperature_cache`.`max_temperature` AS `max_temperature` from `temperature_cache` ;

-- --------------------------------------------------------

--
-- Structure for view `temperature_humidity`
--
DROP TABLE IF EXISTS `temperature_humidity`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `temperature_humidity`  AS  select `temperature_humidity_cache`.`granularityId` AS `granularity`,`temperature_humidity_cache`.`sensor_id` AS `sensor_id`,`temperature_humidity_cache`.`ts` AS `ts`,(`temperature_humidity_cache`.`sum_temperature` / `temperature_humidity_cache`.`count_agg_rows`) AS `temperature`,(`temperature_humidity_cache`.`sum_humidity` / `temperature_humidity_cache`.`count_agg_rows`) AS `humidity`,(`temperature_humidity_cache`.`sum_battery_voltage` / `temperature_humidity_cache`.`count_agg_rows`) AS `sum_battery_voltage/ count_agg_rows`,`temperature_humidity_cache`.`min_temperature` AS `min_temperature`,`temperature_humidity_cache`.`max_temperature` AS `max_temperature`,`temperature_humidity_cache`.`min_humidity` AS `min_humidity`,`temperature_humidity_cache`.`max_humidity` AS `max_humidity`,`temperature_humidity_cache`.`min_battery_voltage` AS `min_battery_voltage`,`temperature_humidity_cache`.`max_battery_voltage` AS `max_battery_voltage` from `temperature_humidity_cache` ;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
