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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Driver {

  private static final Logger LOGGER = LogManager.getLogger(Driver.class);

  public static void main(String[] args) {
    Options options = new Options();
    addOptions(options);

    CommandLineParser parser = new DefaultParser();
    CommandLine cli = null;

    Path inputFolder = null;
    Path outputFolder = null;

    try {
      cli = parser.parse(options, args);

      if (!cli.hasOption('i') || !cli.hasOption('o')) {
        usage(options);
        return;
      }

      inputFolder = Paths.get(cli.getOptionValue('i'));
      if (!Files.exists(inputFolder)) {
        LOGGER.error("Input folder does not exist.");
        return;
      }

      outputFolder = Paths.get(cli.getOptionValue('o'));
      Files.createDirectories(outputFolder);

      Pig2SQL.convert(inputFolder, outputFolder);
    } catch (ParseException e) {
      LOGGER.error("Invalid parameter", e);
    } catch (IOException e) {
      LOGGER.error("Exception occured while reading file", e);
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }

  private static void addOptions(Options options) {
    // Required
    options.addOption("i", "input-directory", true, "input directory");
    options.addOption("o", "output-directory", true, "output directory");
  }

  static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(null);
    formatter.printHelp("pig2sql -i <arg> -o <arg>", options);
  }

}
