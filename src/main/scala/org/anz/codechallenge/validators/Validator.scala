package org.anz.codechallenge.validators

/**
  * All validators will extend this trait
  */
trait Validator {

  /**
    * Perform data validation
    * @return - status of validation
    */
  def validate(): String

}
