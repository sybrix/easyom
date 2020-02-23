package sybrix.easyom


import sybrix.easyom.EasyOM
import model.TestTable
import model.TestTable2


class EasyOMTest extends GroovyTestCase {

        private Properties propFile = new Properties()

        //("classpath:env.properties");
static int pk = 0

        EasyOM em
        boolean  isFirebird = false
        boolean  isMySql = true

        public EasyOMTest() {
                propFile.load(EasyOM.class.getResourceAsStream("/env.properties"))

                em = new EasyOM(propFile)
                em.injectMethods(TestTable.class)
                em.injectMethods(TestTable2.class)
        }

        public void setUp() {
                try {
                        def db = em.getSqlInstance(null)

                        if (isFirebird) {
                                db.execute("""
                                CREATE TABLE `tbltesttable` (
                                  `pk_column` int(11) NOT NULL auto_increment,
                                  `integer_column` integer default NULL,
                                  `small_int_column` smallint(6) default NULL,
                                  `float_column` float(9,3) default NULL,
                                  `double_precision_column` double(15,3) default NULL,
                                  `numeric_column` float(9,3) default NULL,
                                  `decimal_column` decimal(11,0) default NULL,
                                  `date_column` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
                                  `char_column` char(20) default NULL,
                                  `varchar_column` varchar(20) default NULL,
                                  `boolean_column` tinyint(1) default NULL,
                                  `column1` varchar(20) default NULL,
                                  `blob_column` BLOB subtype 0,
                                  PRIMARY KEY  (`pk_column`) ,
                                   UNIQUE KEY `small_int_column` (`small_int_column`)
                                        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
                                """)
                                return
                        } else if (isMySql){
                                def sql = """
                                CREATE TABLE TBLTESTTABLE (
                                          PK_COLUMN BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                          SMALL_INT_COLUMN SMALLINT,
                                          INTEGER_COLUMN INTEGER,
                                          DOUBLE_PRECISION_COLUMN DOUBLE,
                                          NUMERIC_COLUMN FLOAT,
                                          DECIMAL_COLUMN DECIMAL,
                                          DATE_COLUMN DATE,
                                          TIMESTAMP_COLUMN TIMESTAMP,
                                          CHAR_COLUMN CHAR(20) ,
                                          VARCHAR_COLUMN VARCHAR(20) ,
                                          BLOB_COLUMN BLOB,
                                          BOOLEAN_COLUMN BOOLEAN,
                                          FLOAT_COLUMN FLOAT,
                                          COLUMN1 VARCHAR(20) 
                                );
                                """


                                db.execute(sql)

                                db.execute('ALTER TABLE TBLTESTTABLE ADD CONSTRAINT UQ_TBLTESTTABLE UNIQUE (SMALL_INT_COLUMN);')

                               // db.execute('ALTER TABLE TBLTESTTABLE ADD PRIMARY KEY (PK_COLUMN);');

//                                db.execute('CREATE GENERATOR TESTTABLE_PK_COLUMN_GEN_NEW;');
//
//                                db.execute('SET GENERATOR TESTTABLE_PK_COLUMN_GEN_NEW TO 1');
//                                db.execute("""
//                                CREATE TRIGGER BI_TESTTABLE_PK_COLUMN_NEW FOR TBLTESTTABLE
//                                ACTIVE BEFORE INSERT
//                                POSITION 0
//                                AS
//                                BEGIN
//                                  IF (NEW.PK_COLUMN IS NULL) THEN
//                                      NEW.PK_COLUMN = GEN_ID(TESTTABLE_PK_COLUMN_GEN_NEW, 1);
//                                END;
//                        """);
                        } else {
                                db.execute("""
                               CREATE TABLE TBLTESTTABLE (
                                  PK_COLUMN BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) primary key,
                                  SMALL_INT_COLUMN SMALLINT,
                                  INTEGER_COLUMN INTEGER,
                                  DOUBLE_PRECISION_COLUMN DOUBLE PRECISION,
                                  NUMERIC_COLUMN NUMERIC(15, 2),
                                  DECIMAL_COLUMN NUMERIC(15, 2),
                                  DATE_COLUMN TIMESTAMP ,
                                  TIMESTAMP_COLUMN TIMESTAMP,
                                  CHAR_COLUMN CHAR(20) ,
                                  VARCHAR_COLUMN VARCHAR(20) ,
                                  BLOB_COLUMN BLOB,
                                  CLOB_COLUMN CLOB,
                                  BOOLEAN_COLUMN BOOLEAN,
                                  FLOAT_COLUMN FLOAT,
                                  COLUMN1 VARCHAR(20) )
                        """)

                                db.execute("CREATE SEQUENCE id_seq START WITH 1 INCREMENT BY 1")
                        }

                        db.close()

                } catch (Exception e) {
                        dropTable()
                } finally{

                }
        }

        private def insertRecord() {
                em.withTransaction {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 254
                        tbl.integerColumn = 2
                        tbl.floatColumn = 3.1f
                        tbl.doublePrecisionColumn = 4.2
                        tbl.numericColumn = 5
                        tbl.decimalColumn = 6.3
                        tbl.dateColumn = new Date()
                        tbl.charColumn = 'hey now - '
                        //tbl.blobColumn = "David Smith".bytes
                        //tbl.blobColumn = new sybrix.easyom.Blob("David Smith")
                        tbl.boolColumn = false
                        tbl.testMapColumn = "working "
                        tbl.varcharColumn = "varchar"
                        tbl.insert()
                }
        }

//        public void testBlob() {
//                insertRecord()
//                def parameters = [254]
//                def r = "SELECT small_int_column smallIntColumn, char_column charColumn, blob_column blobColumn FROM tblTestTable".executeQuery(TestTable
//                        .class)
//                byte[] data = r.blobColumn[0]
//                //byte data = new byte[l]
//
//                //r.blobColumn.binaryInputStream(data)
//
//                assertEquals(new String(data), "David Smith")
//                assertTrue(r.size() > 0)
//                assertEquals(r[0].smallIntColumn, 254)
//        }


        public void testStringExecuteWithBean() {
                insertRecord()
                def parameters = [254]
                def r = """SELECT small_Int_Column , blob_column  FROM tblTestTable""".executeQuery(TestTable.class)

                assertTrue(r.size() > 0)
                assertEquals( 254, r[0].smallIntColumn)
        }

        public void testTransaction() {
                TestTable tbl = new TestTable()

                tbl.smallIntColumn = 254
                tbl.integerColumn = 2
                tbl.floatColumn = 3.1f
                tbl.doublePrecisionColumn = 4.2
                tbl.numericColumn = 5
                tbl.decimalColumn = 6.3
                tbl.dateColumn = new Date()
                tbl.charColumn = 'hey now - '
                //tbl.blobColumn = new sybrix.easyom.Blob("David Smith")
                tbl.boolColumn = false
                tbl.testMapColumn = "working "
                tbl.timestampColumn = new Date()
                tbl.insert()

                def count = "SELECT count(*) FROM tbltesttable".executeScalar()
                assertTrue(count > 0)
                "DELETE FROM tbltesttable".executeUpdate()

                count = "SELECT count(*) FROM tbltesttable".executeScalar()
                assertTrue('records found before transaction started', count == 0)

                try {
                        withTransaction {
                                tbl.insert()
                                tbl.insert()
                        }
                } catch (Exception e) {

                }

                count = "SELECT count(*) FROM tbltesttable".executeScalar()
                assertTrue('records found after transaction should have failed', count == 0)
        }

        public void testInsertUpdatedColumnsOnly() {

                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 254
                tbl.integerColumn = 2
                tbl.insert(false)

                TestTable tbl2 = TestTable.find([smallIntColumn: 254])
                assertTrue tbl2.smallIntColumn == tbl.smallIntColumn
               // assertTrue tbl2.varcharColumn == 'defaultValue'

                "DELETE FROM tbltesttable".executeUpdate()

                TestTable tbl3 = new TestTable()
                //tbl.pkColumn = pk++
                tbl3.smallIntColumn = 254
                tbl3.integerColumn = 2
                tbl3.save(false)

                TestTable tbl4 = TestTable.find([smallIntColumn: 254])
                assertTrue tbl4.smallIntColumn == tbl3.smallIntColumn
                //assertTrue tbl4.varcharColumn == 'defaultValue'
        }


        public void testInsert() {

                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 254
                tbl.integerColumn = 2
                tbl.floatColumn = 3.1f
                tbl.doublePrecisionColumn = 4.2
                tbl.numericColumn = 5
                tbl.decimalColumn = 6.3
                tbl.dateColumn = new Date()
                tbl.charColumn = 'hey now - '
                tbl.blobColumn = new Blob("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG".getBytes())
                tbl.boolColumn = false
                tbl.testMapColumn = "working "
                tbl.timestampColumn = new Date()
                tbl.insert()

                TestTable tbl2 = TestTable.find([smallIntColumn: 254])
                assertTrue tbl2.smallIntColumn == tbl.smallIntColumn
        }

        public void testBooleanSave() {

                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 9000
                tbl.boolColumn = true
                tbl.insert()

                TestTable tbl2 = TestTable.find([smallIntColumn: 9000])
                assertTrue tbl2.boolColumn == true

//                tbl = new TestTable()
//                tbl.smallIntColumn = 9001
//                tbl.booleanColumn = false
//                tbl.insert()
//
//                tbl2 = TestTable.find([smallIntColumn: 9001])
//                assertTrue tbl2.booleanColumn == false


        }

        public void testBooleanSelect() {

                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 9000
                tbl.boolColumn = true
                tbl.insert()

                def tbl2 = TestTable.findAll([boolColumn: true, smallIntColumn: 9000])
                assertTrue tbl2 != null
        }


        public void testStringExecuteQueryWithPaging() {

                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                def pagedResults = "SELECT small_Int_Column smallIntColumn FROM tblTestTable ORDER BY small_Int_Column".executeQuery([page: 2, pageSize: 2])

                assertTrue(pagedResults.recordCount > 0)
                assertTrue(pagedResults.page == 2)
                assertTrue(pagedResults.pageCount > 0)
                assertTrue(pagedResults.data.size() > 0)

        }

        public void testStringExecuteQueryWithPagingWithResultClass() {

                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                def params = [254]
                def pagedResults = "SELECT small_Int_Column smallIntColumn FROM tblTestTable ORDER BY small_Int_Column".executeQuery([page: 2, pageSize: 2, resultClass: TestTable.class])

                assertTrue(pagedResults.recordCount > 0)
                assertTrue(pagedResults.page == 2)
                assertTrue(pagedResults.pageCount > 0)
                assertTrue(pagedResults.data.size() > 0)
                assertTrue(pagedResults.data[0] instanceof TestTable)
        }

        public void testStringExecuteQueryWithPagingWithResultClassAndParameters() {

                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 500 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                def params = 500L
                def pagedResults = "SELECT small_Int_Column smallIntColumn FROM tblTestTable WHERE small_Int_Column > ${params} ORDER BY small_Int_Column".executeQuery([page: 2, pageSize: 2, resultClass: TestTable.class])

                assertTrue("record count incorrect", pagedResults.recordCount > 0)
                assertTrue("page number incorrect", pagedResults.page == 2)
                assertTrue("page count incorrect", pagedResults.pageCount == 3)

                assertTrue("result set size incorrect", pagedResults.data.size() == 2)
                assertTrue("result is not correct type", pagedResults.data[0] instanceof TestTable)
                assertTrue("resultClass returned incorect value", pagedResults.data[0].smallIntColumn > 500)
        }

        public void testStringExecuteWithParameters() {
                insertRecord()
                def param1 = 254
                def param2 = 2
                def r = "SELECT small_Int_Column smallIntColumn FROM tblTestTable where small_int_column = ${param1} and integer_column = ${param2}".executeQuery()

                assertEquals(r[0].smallIntColumn, 254)
        }

//        public void testStringExecuteWithBeanAndParameters() {
//                String s = "";
//                Object t = String.class;
//
//                insertRecord()
//                def param1 = 254
//                def r = "SELECT small_Int_Column smallIntColumn FROM tblTestTable where small_int_column = ${param1}".executeQuery(TestTable.class)
//
//                assertTrue(r.size() > 0)
//                assertEquals(r[0].smallIntColumn, 254)
//        }


        public void testStringExecuteScalar() {
                insertRecord()
                def val = 'varchar'
                def r = "SELECT count(*) FROM tblTestTable where varchar_Column=${val}".executeScalar()
                assertTrue(r > 0)
        }

        public void testSelect() {
                insertRecord()

                TestTable t = TestTable.find([smallIntColumn: 254])
                assertTrue t.smallIntColumn == 254
        }

        public void testUpdate() {
                insertRecord()

                TestTable t = TestTable.find([smallIntColumn: 254])
                t.timestampColumn = new Date()
                t.testMapColumn = 'val'
                t.charColumn = 'hello'
                t.boolColumn = true

                assertTrue t.save() > 0

                TestTable t2 = TestTable.find([smallIntColumn: 254])
                assertTrue t2.boolColumn == true
        }

        public void testUpdateWithNoColumnsProperty() {
                insertRecord()

                TestTable2 t = TestTable2.find([smallIntColumn: 254])
                t.timestampColumn = new Date()
                t.column1 = 'val2'
                t.charColumn = 'hello'

                assertTrue t.save() > 0

                TestTable2 t2 = TestTable2.find([smallIntColumn: 254])
                assertTrue t2.column1 == 'val2'
        }

        public void testStaticDelete() {
                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 305
                tbl.integerColumn = 306
                tbl.insert()

                assertTrue TestTable.delete([smallIntColumn: 305, integerColumn: 306]) > 0
        }

        public void testStaticDeleteOperator() {
                TestTable tbl = new TestTable()
                //tbl.pkColumn = pk++
                tbl.smallIntColumn = 305
                tbl.integerColumn = 306
                tbl.insert()

                assertTrue TestTable.delete([operator: 'or', smallIntColumn: 305, integerColumn: 306]) == 1
        }


        public void testDelete() {
                insertRecord()

                TestTable t = TestTable.find([smallIntColumn: 254])
                assertTrue t.delete() > 0
        }

        public void testFindAll() {
                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                def results = TestTable.findAll([smallIntColumn: 300, orderBy: 'smallIntColumn , testMapColumn DESC'])
                assertTrue results.size() > 0

                //TestTable.delete([smallIntColumn: 300])
        }

        public void testFindAllNull() {
                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = null
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                def results = TestTable.findAll([dateColumn: null, smallIntColumn: 300])
                assertTrue results.size() > 0
                assertTrue results.size() == 1
                //TestTable.delete([smallIntColumn: 300])
        }


        public void testFindAllOperator() {
                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working" + i
                        tbl.insert()
                }

                def results = TestTable.findAll([smallIntColumn: 301, testMapColumn: 'working2', orderBy: 'smallIntColumn , testMapColumn DESC', operator: 'OR'])
                assertTrue results.size() == 2

                //TestTable.delete([smallIntColumn: 300])
        }


        public void testFindAllWithPaging() {
                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.integerColumn = 300
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                Map pagedResults = TestTable.findAll([integerColumn: 300, orderBy: 'integerColumn , testMapColumn DESC', page: 2, pageSize: 3])

                assertTrue(pagedResults.recordCount > 0)
                assertTrue(pagedResults.page == 2)
                assertTrue(pagedResults.pageCount > 0)
                assertTrue(pagedResults.results.size() > 0)
                assertTrue(pagedResults.results[0] instanceof TestTable)

                TestTable.delete([integerColumn: 300])
        }

//        public void testSecondTable() {
//                Profile p = new Profile(lastName: 'Smith', profileId: 0)
//                p.insert()
//
//                Profile.delete([profileId: 0])
//        }

        public void testList() {
                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }
//                insertdef w = new WhereClauseParameters()
//                w.orderBy = "smallInt"

                List results = TestTable.list([orderBy: "smallIntColumn"])
                assertTrue(results.size() > 0)

                TestTable.delete([smallIntColumn: 300])
        }

        public void testListWithPaging() {

                for (i in 0..5) {
                        TestTable tbl = new TestTable()
                        //tbl.pkColumn = pk++
                        tbl.smallIntColumn = 300 + i
                        tbl.dateColumn = new Date()
                        tbl.testMapColumn = "working"
                        tbl.insert()
                }

                Map pagedResults = TestTable.list(orderBy: 'smallIntColumn', page: 2, pageSize: 2)

                assertTrue(pagedResults.recordCount > 0)
                assertTrue(pagedResults.page == 2)
                assertTrue(pagedResults.pageCount > 0)
                assertTrue(pagedResults.results.size() > 0)
        }

        public void testStringExecuteUpdate() {
                def ct = "SELECT COUNT(*) FROM tblTestTable".executeScalar()
                def r = "DELETE FROM tblTestTable".executeUpdate()
                System.out.println("ct = " + ct)
                assertTrue(r >= 0)
        }

        def paging(String sql, Object[] param, String totalCountSql, int page, int pageSize) {
                def totalCount = totalCountSql.executeScalar();
                List results = executeQuery()


                int pageCount = (int) Math.ceil(totalCount / pageSize);
                recordCount = results.size();
                setCurrentItem();

        }

        protected void tearDown() {
                dropTable()
        }

        private def dropTable() {
                def db = em.getSqlInstance(null)
                try {

                        if (isFirebird == true) {
                                db.executeUpdate('DROP TABLE TBLTESTTABLE;');
                                db.executeUpdate('DROP GENERATOR TESTTABLE_PK_COLUMN_GEN_NEW;');
                        } else {
                                try {
                                        db.executeUpdate('DROP TABLE TBLTESTTABLE');
                                }catch (e){

                                }
                                if (!isMySql ) {
                                        try {
                                                db.executeUpdate('DROP SEQUENCE id_seq RESTRICT');
                                        } catch (e) {

                                        }
                                }

                        }
                }catch (Exception e){
                }finally {
                        db.close()
                }
        }

}
