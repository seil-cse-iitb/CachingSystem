-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- Host: 10.129.149.22
-- Generation Time: Feb 05, 2020 at 05:53 PM
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
-- Database: `seil_sl_cache1_prod`
--
CREATE DATABASE IF NOT EXISTS `seil_sl_cache1_prod` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `seil_sl_cache1_prod`;

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
  KEY `granularityId` (`granularityId`,`sensor_id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

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
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
