package org.anz.codechallenge.schema

import java.util.Optional

case class Schema(
                    columns : Array[FileSchema],
                    primary_keys : Array[String]
                 )


case class FileSchema (
                    name : String,
                    `type` : String,
                    format : String,
                    mandatory : String
                  )
