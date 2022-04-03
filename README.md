File Validation code challenge

Application Requirements:

    1 Java 11 and Spark 3.x
    2 Maven project
    Note: project must be able to create fat jar file
    3 Unit tests with at least 50% code coverage
    4 Self-executing jar file. It must be named anz-code-challenge-with-dependencies.jar
    5 All data manipulations can either be written using JavaRDD or Dataset<Row>
    6 There must be a README file in the repository with all build instructions
    7 List of assumptions that you have made in creating your program

Non-Functional criteria:

    • Naming standards used
    • Comments
    • Performance and scalability
    • Project structure and general coding conventions
    • Demonstrated knowledge of Git (branching, commit messages, pull requests)


Assumptions:

    1) Schema file aus-capitals.json provided had "State/Territory" repeating. Removed the repeat
    2) The tag files for invalid scenarios where not having correct file names . Created respective tag files while testing the scenarios
    3) The program would be called per file by passing the file path, schema path and tag file path



Features to be handled:

    1) Output file name matching with what is specified in the scenarios
    2) logger statements replacing println
    3) More code coverage