-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- Host: 10.129.149.22
-- Generation Time: Jan 26, 2020 at 10:44 AM
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
  `sensor_id` varchar(20) DEFAULT NULL,
  `sum_power` double DEFAULT NULL,
  `sum_voltage` double DEFAULT NULL,
  `sum_current` double DEFAULT NULL,
  `energy_consumed` double DEFAULT NULL,
  `min_power` double DEFAULT NULL,
  `max_power` double DEFAULT NULL,
  `min_voltage` double DEFAULT NULL,
  `max_voltage` double DEFAULT NULL,
  `min_current` double DEFAULT NULL,
  `max_current` double DEFAULT NULL,
  `slot_energy_consumed` double DEFAULT NULL,
  `count_agg_rows` bigint(20) NOT NULL,
  `ts` double DEFAULT NULL,
  `granularityId` varchar(100) NOT NULL,
  KEY `granularityId` (`granularityId`,`sensor_id`,`ts`),
  KEY `sensor_id` (`sensor_id`,`ts`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
