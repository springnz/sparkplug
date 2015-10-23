package springnz.sparkplug.testkit

import springnz.sparkplug.data.JdbcDataFrameSource

class JdbcTestDataSource(projectFolder: String, dataBaseName: String, tableName: String, sample: Boolean = true)
  extends PersistableDFProcess(projectFolder: String, dataBaseName + "-" + tableName, sample)(new JdbcDataFrameSource(dataBaseName, tableName))
