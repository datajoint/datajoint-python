USE djtest_v0_11;
-- MySQL dump 10.13  Distrib 5.7.26, for Linux (x86_64)
--
-- Host: localhost    Database: djtest_v0_11
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
-- Table structure for table `#ext_blob`
--

DROP TABLE IF EXISTS `#ext_blob`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `#ext_blob` (
  `name` varchar(30) NOT NULL,
  `payload` char(51) NOT NULL COMMENT ':external:',
  PRIMARY KEY (`name`),
  KEY `payload` (`payload`),
  CONSTRAINT `#ext_blob_ibfk_1` FOREIGN KEY (`payload`) REFERENCES `~external` (`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `#ext_blob`
--

LOCK TABLES `#ext_blob` WRITE;
/*!40000 ALTER TABLE `#ext_blob` DISABLE KEYS */;
INSERT INTO `#ext_blob` VALUES ('np.array','KbNUSXF_zvL-j7LpD9EM1JgKS6qmEYHh3s7O-qsw47c'),('image','wdiE3uQUBGThCDKzyisNoKqdXNxoeV7EiTTI_uPnZ0Q');
/*!40000 ALTER TABLE `#ext_blob` ENABLE KEYS */;
UNLOCK TABLES;

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
INSERT INTO `~external` VALUES ('KbNUSXF_zvL-j7LpD9EM1JgKS6qmEYHh3s7O-qsw47c',950,'2019-07-12 21:37:36'),('wdiE3uQUBGThCDKzyisNoKqdXNxoeV7EiTTI_uPnZ0Q',96615,'2019-07-12 21:37:38');
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
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='event logging table for `djtest_v0_11`';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `~log`
--

LOCK TABLES `~log` WRITE;
/*!40000 ALTER TABLE `~log` DISABLE KEYS */;
INSERT INTO `~log` VALUES ('2019-07-12 21:37:31','0.11.1py','root@172.168.1.4','a0cda75c134b','Declared `djtest_v0_11`.`~log`'),('2019-07-12 21:37:32','0.11.1py','root@172.168.1.4','a0cda75c134b','Declared `djtest_v0_11`.`~external`');
/*!40000 ALTER TABLE `~log` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-07-12 21:39:05
