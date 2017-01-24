-- MySQL dump 10.11
--
-- Host: oriole    Database: stocks
-- ------------------------------------------------------
-- Server version	5.0.45-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `groups`
--

DROP TABLE IF EXISTS `groups`;
CREATE TABLE `groups` (
  `id` smallint(3) unsigned NOT NULL auto_increment,
  `symbol` varchar(16) NOT NULL default '',
  `name` varchar(128) default NULL,
  `suffix` varchar(6) default NULL,
  `list_url` varchar(128) default NULL,
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `symbol` (`symbol`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `message_parents`
--

DROP TABLE IF EXISTS `message_parents`;
CREATE TABLE `message_parents` (
  `parent_url` varchar(2000) NOT NULL default '',
  `child_id` bigint(20) unsigned NOT NULL,
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  PRIMARY KEY  (`parent_url`(650),`child_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `message_urls`
--

DROP TABLE IF EXISTS `message_urls`;
CREATE TABLE `message_urls` (
  `url` varchar(2000) NOT NULL default '',
  `stock_id` int(7) unsigned NOT NULL default '0',
  `source_id` int(7) unsigned NOT NULL default '0',
  `status` tinyint(2) NOT NULL default '0',
  `duplicate_id` bigint(20) unsigned default NULL,
  `msg_date` timestamp NOT NULL default '0000-00-00 00:00:00',
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  PRIMARY KEY  (`url`(650),`stock_id`),
  KEY `idx_url` (`url`(650)),
  KEY `idx_stock_id` (`stock_id`),
  KEY `idx_source_id` (`source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `messages`
--

DROP TABLE IF EXISTS `messages`;
CREATE TABLE `messages` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `stock_id` int(7) unsigned NOT NULL default '0',
  `title` varchar(255) NOT NULL default '',
  `body` text,
  `sentiment` varchar(16) default NULL,
  `url` text NOT NULL,
  `user` varchar(64) NOT NULL default '',
  `msg_date` timestamp NOT NULL default '0000-00-00 00:00:00',
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  `source_id` int(7) unsigned NOT NULL default '0',
  `parent_id` bigint(20) unsigned default NULL,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `idx_msg` (`stock_id`,`msg_date`,`user`,`source_id`),
  KEY `idx_source_id` (`source_id`),
  KEY `idx_stock_id` (`stock_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3007485 DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Temporary table structure for view `penny_stocks`
--

DROP TABLE IF EXISTS `penny_stocks`;
/*!50001 DROP VIEW IF EXISTS `penny_stocks`*/;
/*!50001 CREATE TABLE `penny_stocks` (
  `id` int(7) unsigned,
  `symbol` varchar(16),
  `full_symbol` varchar(22),
  `name` varchar(128)
) */;

--
-- Table structure for table `quotes`
--

DROP TABLE IF EXISTS `quotes`;
CREATE TABLE `quotes` (
  `stock_id` int(7) unsigned NOT NULL default '0',
  `price` decimal(10,4) unsigned NOT NULL default '0.0000',
  `volume` bigint(20) unsigned NOT NULL default '0',
  `ask` decimal(10,4) unsigned default NULL,
  `ask_size` bigint(20) unsigned default NULL,
  `bid` decimal(10,4) unsigned default NULL,
  `bid_size` bigint(20) unsigned default NULL,
  `cdate` timestamp NOT NULL default '0000-00-00 00:00:00',
  PRIMARY KEY  (`stock_id`,`cdate`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `sources`
--

DROP TABLE IF EXISTS `sources`;
CREATE TABLE `sources` (
  `id` int(7) unsigned NOT NULL auto_increment,
  `name` varchar(32) NOT NULL default '',
  `full_name` varchar(32) NOT NULL default '',
  `base_url` varchar(255) NOT NULL default '',
  `prefix_url` varchar(32) default NULL,
  `use_suffix` tinyint(1) NOT NULL default '0',
  `unique_url` tinyint(1) NOT NULL default '1',
  `group_url` tinyint(1) unsigned NOT NULL default '0',
  `enabled` tinyint(1) NOT NULL default '1',
  `fetcher` varchar(8) NOT NULL default '',
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `stock_groups`
--

DROP TABLE IF EXISTS `stock_groups`;
CREATE TABLE `stock_groups` (
  `group_id` smallint(3) unsigned NOT NULL default '0',
  `stock_id` int(7) unsigned NOT NULL default '0',
  PRIMARY KEY  (`group_id`,`stock_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `stock_switch`
--

DROP TABLE IF EXISTS `stock_switch`;
CREATE TABLE `stock_switch` (
  `old_id` int(7) unsigned NOT NULL,
  `new_id` int(7) unsigned NOT NULL,
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  PRIMARY KEY  (`old_id`,`new_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Table structure for table `stocks`
--

DROP TABLE IF EXISTS `stocks`;
CREATE TABLE `stocks` (
  `id` int(7) unsigned NOT NULL auto_increment,
  `name` varchar(128) NOT NULL default '',
  `symbol` varchar(16) NOT NULL default '',
  `industry_id` smallint(3) unsigned NOT NULL default '0',
  `history_start` date default NULL,
  `market_cap` bigint(20) unsigned default NULL,
  `avg_volume_3mon` bigint(20) unsigned default NULL,
  `avg_volume_10day` bigint(20) unsigned default NULL,
  `outstanding_shares` bigint(20) unsigned default NULL,
  `float_shares` bigint(20) unsigned default NULL,
  `last_sec_filing` date default NULL,
  `valid` tinyint(1) NOT NULL default '1',
  `cdate` timestamp NOT NULL default CURRENT_TIMESTAMP,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `symbol` (`symbol`)
) ENGINE=InnoDB AUTO_INCREMENT=25639 DEFAULT CHARSET=latin1 PACK_KEYS=1;

--
-- Final view structure for view `penny_stocks`
--

/*!50001 DROP TABLE IF EXISTS `penny_stocks`*/;
/*!50001 DROP VIEW IF EXISTS `penny_stocks`*/;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`pavlo`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `penny_stocks` AS (select `stocks`.`id` AS `id`,`stocks`.`symbol` AS `symbol`,if((`groups`.`suffix` is not null),concat(`stocks`.`symbol`,`groups`.`suffix`),`stocks`.`symbol`) AS `full_symbol`,`stocks`.`name` AS `name` from ((`stocks` join `stock_groups`) join `groups`) where ((`stocks`.`id` = `stock_groups`.`stock_id`) and (`stock_groups`.`group_id` in (3,4)) and (`groups`.`id` = `stock_groups`.`group_id`) and (`stocks`.`valid` = 1))) */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2009-06-17 16:59:24
