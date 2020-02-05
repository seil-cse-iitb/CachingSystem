-- phpMyAdmin SQL Dump
-- version 4.5.4.1deb2ubuntu2.1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Feb 05, 2020 at 11:40 PM
-- Server version: 5.7.27-0ubuntu0.16.04.1
-- PHP Version: 7.0.33-0ubuntu0.16.04.9

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `seil_sensor_data`
--
CREATE DATABASE IF NOT EXISTS `seil_sensor_data` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `seil_sensor_data`;

-- --------------------------------------------------------

--
-- Stand-in structure for view `dht_7_view`
--
CREATE TABLE IF NOT EXISTS `dht_7_view` (
`sensor_id` varchar(30)
,`ts` double
,`temperature` double
,`humidity` double
,`battery_voltage` double
);

-- --------------------------------------------------------

--
-- Stand-in structure for view `rish_1_view`
--
CREATE TABLE IF NOT EXISTS `rish_1_view` (
`sensor_id` varchar(20)
,`ts` double
,`voltage_1` double
,`voltage_2` double
,`voltage_3` double
,`current_1` double
,`current_2` double
,`current_3` double
,`power_1` double
,`power_2` double
,`power_3` double
,`power_factor_1` double
,`power_factor_2` double
,`power_factor_3` double
,`frequency` double
,`energy_consumed` double
);

-- --------------------------------------------------------

--
-- Stand-in structure for view `sch_3_view`
--
CREATE TABLE IF NOT EXISTS `sch_3_view` (
`sensor_id` varchar(20)
,`ts` double
,`voltage_1` double
,`voltage_2` double
,`voltage_3` double
,`current_1` double
,`current_2` double
,`current_3` double
,`power_1` double
,`power_2` double
,`power_3` double
,`power_factor_1` double
,`power_factor_2` double
,`power_factor_3` double
,`energy_consumed` double
);

-- --------------------------------------------------------

--
-- Stand-in structure for view `temp_5_view`
--
CREATE TABLE IF NOT EXISTS `temp_5_view` (
`sensor_id` varchar(30)
,`ts` double
,`temperature` double
);

-- --------------------------------------------------------

--
-- Structure for view `dht_7_view`
--
DROP TABLE IF EXISTS `dht_7_view`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `dht_7_view`  AS  select `dht_7`.`new_sensor_id` AS `sensor_id`,`dht_7`.`TS` AS `ts`,`dht_7`.`temperature` AS `temperature`,`dht_7`.`humidity` AS `humidity`,`dht_7`.`battery_voltage` AS `battery_voltage` from `dht_7` ;

-- --------------------------------------------------------

--
-- Structure for view `rish_1_view`
--
DROP TABLE IF EXISTS `rish_1_view`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `rish_1_view`  AS  select `rish_1`.`sensor_id` AS `sensor_id`,`rish_1`.`TS` AS `ts`,`rish_1`.`V1` AS `voltage_1`,`rish_1`.`V2` AS `voltage_2`,`rish_1`.`V3` AS `voltage_3`,`rish_1`.`A1` AS `current_1`,`rish_1`.`A2` AS `current_2`,`rish_1`.`A3` AS `current_3`,`rish_1`.`W1` AS `power_1`,`rish_1`.`W2` AS `power_2`,`rish_1`.`W3` AS `power_3`,`rish_1`.`PF1` AS `power_factor_1`,`rish_1`.`PF2` AS `power_factor_2`,`rish_1`.`PF3` AS `power_factor_3`,`rish_1`.`F` AS `frequency`,`rish_1`.`FwdWh` AS `energy_consumed` from `rish_1` ;

-- --------------------------------------------------------

--
-- Structure for view `sch_3_view`
--
DROP TABLE IF EXISTS `sch_3_view`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `sch_3_view`  AS  select `sch_3`.`sensor_id` AS `sensor_id`,`sch_3`.`TS` AS `ts`,`sch_3`.`V1` AS `voltage_1`,`sch_3`.`V2` AS `voltage_2`,`sch_3`.`V3` AS `voltage_3`,`sch_3`.`A1` AS `current_1`,`sch_3`.`A2` AS `current_2`,`sch_3`.`A3` AS `current_3`,`sch_3`.`W1` AS `power_1`,`sch_3`.`W2` AS `power_2`,`sch_3`.`W3` AS `power_3`,`sch_3`.`PF1` AS `power_factor_1`,`sch_3`.`PF2` AS `power_factor_2`,`sch_3`.`PF3` AS `power_factor_3`,`sch_3`.`FwdWh` AS `energy_consumed` from `sch_3` ;

-- --------------------------------------------------------

--
-- Structure for view `temp_5_view`
--
DROP TABLE IF EXISTS `temp_5_view`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `temp_5_view`  AS  select `temp_5`.`new_sensor_id` AS `sensor_id`,`temp_5`.`TS` AS `ts`,`temp_5`.`temperature` AS `temperature` from `temp_5` ;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
