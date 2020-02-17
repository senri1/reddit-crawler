import pymysql
import datetime as dt

class LogDB:

    def ifTableExist(self, tablename):
        stmt = "SHOW TABLES LIKE '{name}'".format(name=tablename)
        self.cursor.execute(stmt)
        result = self.cursor.fetchone()
        if result:
            print("Table exist.")
            return True
        else:
            print("Table does not exist.")
            return False
    
    def createTable(self):
        print("Creating table...")
        stmt = """
            CREATE TABLE `{}` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
            `start_date` DATETIME DEFAULT NULL,
            `end_date` DATETIME DEFAULT NULL,
            `crawl_start` DATETIME NULL DEFAULT NULL,
            `crawl_end` DATETIME NULL DEFAULT NULL,
            `context` text,
            `length` INT UNSIGNED NOT NULL DEFAULT 0,
            `success` boolean,
            PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """.format(self.tablename)
        try:
            self.cursor.execute(stmt)
            self.connection.commit()
            print("Table created")
        except Exception as e:
            print("Could not create table failed with error: \n{})".format(e))

    def __init__(self, host, user, password, db, tablename):
        self.connection = pymysql.connect(
                            host=host,
                            user=user,
                            password=password,
                            db=db)
        self.tablename = tablename
        self.cursor = self.connection.cursor()
        if not self.ifTableExist(tablename):
            self.createTable()

    def insertRow(self, sql, values):
        # Insert row to DB
        # sql: SQL prepared statement 
        # values: prepared values
        # return success, rowID
        try:
            self.cursor.execute(sql, values)
            self.connection.commit()
            return True, self.cursor.lastrowid
        except pymysql.err.InternalError:
            return False, None
    
    def deleteAllRows(self):
        sql = "TRUNCATE {}".format(self.tablename)
        try:
            self.cursor.execute(sql)
            return True
        except pymysql.err.InternalError:
            return False
    
    def updateRow(self, sql, values):
        # update row
        # sql: SQL prepared statement 
        # values: prepared values
        # return success, message
        try:
            self.cursor.execute(sql, values)
            self.connection.commit()
            return True, str(self.cursor.rowcount)+" record(s) affected"
        except pymysql.err.InternalError:
            return False, None

    def query(self, sql):
        # Fetch many rows
        try:
            self.cursor.execute(sql)
            return True, self.cursor.fetchall()
        except pymysql.err.InternalError:
            return False, None
        
    def queryOne(self, sql):
        # Fetch 1 row
        try:
            self.cursor.execute(sql)
            return True, self.cursor.fetchone()
        except pymysql.err.InternalError:
            return False, None

class RedditLogDB(LogDB):

    def __init__(self, host, user, password, db, tablename):
        super().__init__(host, user, password, db, tablename)
    
    def startCrawl(self, start_date, end_date, context):
        # Insert row to DB
        # stmt: SQL statement
        # return success, message
        sql = "INSERT INTO {} (start_date, end_date, crawl_start, context) VALUES (%s, %s, %s, %s)".format(self.tablename)
        val = (start_date, end_date, dt.datetime.utcnow(), context)
        return self.insertRow(sql, val)

    def endCrawl(self, id, length, success):
        sql = "UPDATE {} SET success=%s, crawl_end=%s, length=%s WHERE id=%s".format(self.tablename)
        val = (success, dt.datetime.utcnow(), length, id)
        return self.updateRow(sql, val)

    def earliestQuery(self):
        # reverse Find the next day to crawl
        # return None if no record
        sql = "SELECT start_date FROM {} ORDER BY start_date LIMIT 1".format(self.tablename)
        success, result = self.queryOne(sql)
        if result:
            return result
        else:
            return None