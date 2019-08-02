USE djtest_blob_migrate;
-- MySQL dump 10.13  Distrib 5.7.26, for Linux (x86_64)
--
-- Host: localhost    Database: djtest_blob_migrate
-- ------------------------------------------------------
-- Server version	5.7.26

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
-- Table structure for table `~external`
--

DROP TABLE IF EXISTS `~external`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `~external` (
  `hash` char(51) NOT NULL COMMENT 'the hash of stored object + store name',
  `size` bigint(20) unsigned NOT NULL COMMENT 'size of object in bytes',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'automatic timestamp',
  PRIMARY KEY (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='external storage tracking';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `~external`
--

LOCK TABLES `~external` WRITE;
/*!40000 ALTER TABLE `~external` DISABLE KEYS */;
INSERT INTO `~external` VALUES ('e46pnXQW9GaCKbL3WxV1crGHeGqcE0OLInM_TTwAFfwlocal',237,'2019-07-31 17:55:01'),('FoRROa2LWM6_wx0RIQ0J-LVvgm256cqDQfJa066HoTEshared',37,'2019-07-31 17:55:01'),('NmWj002gtKUkt9GIBwzn6Iw3x6h7ovlX_FfELbfjwRQshared',53,'2019-07-31 17:55:01'),('Ue9c89gKVZD7xPOcHd5Lz6mARJQ50xT1G5cTTX4h0L0shared',53,'2019-07-31 17:55:01'),('_3A03zPqfVhbn0rhlOJYGNivFJ4uqYuHaeQBA-V8PKA',237,'2019-07-31 17:55:01'),('_Fhi2GUBB0fgxcSP2q-isgncIUTdgGK7ivHiySAU_94',40,'2019-07-31 17:55:01'),('_Fhi2GUBB0fgxcSP2q-isgncIUTdgGK7ivHiySAU_94local',40,'2019-07-31 17:55:01');
/*!40000 ALTER TABLE `~external` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `~log`
--

DROP TABLE IF EXISTS `~log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `~log` (
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `version` varchar(12) NOT NULL COMMENT 'datajoint version',
  `user` varchar(255) NOT NULL COMMENT 'user@host',
  `host` varchar(255) NOT NULL DEFAULT '' COMMENT 'system hostname',
  `event` varchar(255) NOT NULL DEFAULT '' COMMENT 'custom message',
  PRIMARY KEY (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='event logging table for `djtest_blob_migrate`';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `~log`
--

LOCK TABLES `~log` WRITE;
/*!40000 ALTER TABLE `~log` DISABLE KEYS */;
INSERT INTO `~log` VALUES ('2019-07-31 17:54:49','0.11.1py','root@172.168.1.4','297df05ab17c','Declared `djtest_blob_migrate`.`~log`'),('2019-07-31 17:54:54','0.11.1py','root@172.168.1.4','297df05ab17c','Declared `djtest_blob_migrate`.`~external`'),('2019-07-31 17:54:55','0.11.1py','root@172.168.1.4','297df05ab17c','Declared `djtest_blob_migrate`.`b`');
/*!40000 ALTER TABLE `~log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `a`
--

DROP TABLE IF EXISTS `a`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `a` (
  `id` int(11) NOT NULL,
  `blob_external` char(51) NOT NULL COMMENT ':external:uses S3',
  `blob_share` char(51) NOT NULL COMMENT ':external-shared:uses S3',
  PRIMARY KEY (`id`),
  KEY `blob_external` (`blob_external`),
  KEY `blob_share` (`blob_share`),
  CONSTRAINT `a_ibfk_1` FOREIGN KEY (`blob_external`) REFERENCES `~external` (`hash`),
  CONSTRAINT `a_ibfk_2` FOREIGN KEY (`blob_share`) REFERENCES `~external` (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `a`
--

LOCK TABLES `a` WRITE;
/*!40000 ALTER TABLE `a` DISABLE KEYS */;
INSERT INTO `a` VALUES (0,'_3A03zPqfVhbn0rhlOJYGNivFJ4uqYuHaeQBA-V8PKA','NmWj002gtKUkt9GIBwzn6Iw3x6h7ovlX_FfELbfjwRQshared'),(1,'_Fhi2GUBB0fgxcSP2q-isgncIUTdgGK7ivHiySAU_94','FoRROa2LWM6_wx0RIQ0J-LVvgm256cqDQfJa066HoTEshared');
/*!40000 ALTER TABLE `a` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `b`
--

DROP TABLE IF EXISTS `b`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `b` (
  `id` int(11) NOT NULL,
  `blob_local` char(51) NOT NULL COMMENT ':external-local:uses files',
  `blob_share` char(51) NOT NULL COMMENT ':external-shared:uses S3',
  PRIMARY KEY (`id`),
  KEY `blob_local` (`blob_local`),
  KEY `blob_share` (`blob_share`),
  CONSTRAINT `b_ibfk_1` FOREIGN KEY (`blob_local`) REFERENCES `~external` (`hash`),
  CONSTRAINT `b_ibfk_2` FOREIGN KEY (`blob_share`) REFERENCES `~external` (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `b`
--

LOCK TABLES `b` WRITE;
/*!40000 ALTER TABLE `b` DISABLE KEYS */;
INSERT INTO `b` VALUES (0,'e46pnXQW9GaCKbL3WxV1crGHeGqcE0OLInM_TTwAFfwlocal','Ue9c89gKVZD7xPOcHd5Lz6mARJQ50xT1G5cTTX4h0L0shared'),(1,'_Fhi2GUBB0fgxcSP2q-isgncIUTdgGK7ivHiySAU_94local','FoRROa2LWM6_wx0RIQ0J-LVvgm256cqDQfJa066HoTEshared');
/*!40000 ALTER TABLE `b` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-07-31 18:16:40
