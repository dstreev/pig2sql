/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcamachor.pig2sql;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.piglet.PigConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class containing methods to convert from Pig programs into SQL queries.
 */
public class Pig2SQL {

  private static final Logger LOGGER = LogManager.getLogger(Pig2SQL.class);

  private static final SqlDialect HIVE_SQL_DIALECT = new HiveSqlDialect(
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
          .withDatabaseMajorVersion(3) // TODO: should not be hardcoded
          .withDatabaseMinorVersion(1)
          .withIdentifierQuoteString("`")
          .withDataTypeSystem(new HiveTypeSystemImpl())
          .withNullCollation(NullCollation.LAST)) {
    @Override
    protected boolean allowsAs() {
      return true;
    }

    @Override
    public boolean supportsCharSet() {
      return false;
    }
  };
  private static final Pattern PATTERN_VARCHAR =
      Pattern.compile("VARCHAR\\(2147483647\\)");
  private static final Pattern PATTERN_TIMESTAMP =
      Pattern.compile("TIMESTAMP\\(9\\)");

  protected static void convert(Path inputFolder, Path outputFolder)
      throws Exception {
    final PigConverter converter = PigConverter.create(createConfig());
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputFolder, "*.pig")) {
      for (Path inputFilePath : stream) {
        final File inputFile = inputFilePath.toFile();
        String fileName = inputFile.getName();
        fileName = fileName.substring(0, fileName.indexOf('.'));
        byte[] mapData = Files.readAllBytes(inputFilePath);
        String script = new String(mapData, Charset.defaultCharset());
        final List<String> sqlStmts =
            converter.pigToSql(script, HIVE_SQL_DIALECT);
        File outputFile = new File(outputFolder.toString(), fileName + ".sql");
        try (PrintWriter pw = new PrintWriter(outputFile)) {
          for (String sql : sqlStmts) {
            sql = PATTERN_VARCHAR.matcher(sql).replaceAll("STRING"); // VARCHAR(INTEGER.MAX) -> STRING
            sql = PATTERN_TIMESTAMP.matcher(sql).replaceAll("TIMESTAMP"); // TIMESTAMP(9) -> TIMESTAMP
            pw.println(sql + ";");
          }
          LOGGER.info("Converted " + inputFile + " into " + outputFile);
        }
      }
    }
  }

  private static FrameworkConfig createConfig() {
    final Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT);
    return configBuilder.build();
  }
}
