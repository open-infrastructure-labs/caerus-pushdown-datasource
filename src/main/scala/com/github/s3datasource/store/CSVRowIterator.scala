/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.s3datasource.store

import java.io.BufferedReader
import java.util

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory
import com.univocity.parsers.csv._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class CSVRowIterator(rowReader: BufferedReader,
                     schema: StructType)
  extends Iterator[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)

  private def parseLine(line: String): InternalRow = {
    var row = new Array[Any](schema.fields.length)
    var value: String = ""
    var index = 0
    var fieldStart = 0
    while (index < schema.fields.length && fieldStart < line.length) {
      if (line(fieldStart) != '\"') {
        var fieldEnd = line.substring(fieldStart).indexOf(",")
        if (fieldEnd == -1) {
          // field is from here to the end of the line
          value = line.substring(fieldStart)
          // Next field start is after comma
          fieldStart = line.length
        } else {
          // field is from start (no skipping) to just before ,
          value = line.substring(fieldStart, fieldStart + fieldEnd)
          // Next field start is after comma
          fieldStart = fieldStart + fieldEnd + 1
        }
      } else {
        // Search from +1 (after ") to next quote
        var fieldEnd = line.substring(fieldStart + 1).indexOf("\"")
        // Field range is from after " to just before (-1) next quote
        value = line.substring(fieldStart + 1, fieldStart + fieldEnd + 1)
        // Next field start is after quote and comma
        fieldStart = fieldStart + 1 + fieldEnd + 2
      }
      val field = schema.fields(index)
      row(index) = TypeCast.castTo(value, field.dataType,
                                   field.nullable)
      index += 1
    }
    InternalRow.fromSeq(row.toSeq)
  }

  /* We have the option of parsing ourselves or
   * using the univocity parser.  For now we use manual method for
   * performance reasons.
  private val settings: CsvParserSettings = new CsvParserSettings()
  private val parser: CsvParser = new CsvParser(settings);

  private def parseLineWithParser(line: String): InternalRow = {
    val record = parser.parseRecord(line)

    var row = new Array[Any](schema.fields.length)
    var index = 0
    while (index < schema.fields.length) {
      val field = schema.fields(index)
      row(index) = TypeCast.castTo(record.getString(index), field.dataType,
        field.nullable)
      index += 1
    }
    InternalRow.fromSeq(row.toSeq)
  } */

  private var nextRow: InternalRow = {
    val firstRow = getNextRow()
    firstRow
  }
  private def getNextRow(): InternalRow = {
    var line: String = null
    if ({line = rowReader.readLine(); line == null}) {
      InternalRow.empty
    } else {
      parseLine(line)
    }
  }
  override def hasNext: Boolean = {
    nextRow.numFields > 0
  }

  override def next: InternalRow = {
    val row = nextRow
    nextRow = getNextRow()
    row
  }

  def close(): Unit = Unit
}