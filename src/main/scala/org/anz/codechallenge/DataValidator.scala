package org.anz.codechallenge

object DataValidator {

  def main(args: Array[String]): Unit = {

    if(args.length < 4) {
      println("Incorrect arguments. Please pass schema, data, tag and output.")
      return
    }

    val schema = args(0)
    val data = args(1)
    val tag = args(2)
    val output = args(3)


  }

}


