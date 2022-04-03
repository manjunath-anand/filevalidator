package org.anz.codechallenge.schema

/**
  * Input metadata representation
  * @param schema - Input schema file path
  * @param data - Input data file path
  * @param tag - Input tag file path
  * @param output - Output path after validation
  */
case class Metadata (
                      schema: String,
                      data : String,
                      tag : String,
                      output : String
                    )
