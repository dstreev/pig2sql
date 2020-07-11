Pig2SQL
===========

Program that relies on [Apache Calcite](https://calcite.apache.org/) Piglet module to generate Hive SQL queries from Pig scripts.

Command Line Tool
-----------------------

After running ```mvn package``` in the top level directory, run ```pig2sql``` to display the usage options.

```sh
usage: pig2sql -i <arg> -o <arg>

  Mandatory arguments:
    -i,--input-directory <arg>    input directory
    -o,--output-directory <arg>   output directory
```

The input directory should contain Pig scripts with `.pig` extension. The output directory produces SQL scripts with `.sql` extension.
