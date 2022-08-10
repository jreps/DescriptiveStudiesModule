# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of DescriptiveStudiesModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Module methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

execute <- function(jobContext) {
  rlang::inform("Validating inputs")
  inherits(jobContext, 'list')

  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$sharedResources)) {
    stop("Shared resources not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }
  
  resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
  
  rlang::inform("Executing DescriptiveStudies")
  moduleInfo <- getModuleInfo()
  
  # run the models
  DescriptiveStudies::runCharacterizationAnalyses(
    connectionDetails = jobContext$moduleExecutionSettings$connectionDetails, 
    targetDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    targetTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
    outcomeDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    outcomeTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
    cdmDatabaseSchema = jobContext$moduleExecutionSettings$cdmDatabaseSchema, 
    characterizationSettings = jobContext$settings, 
    databaseId = jobContext$moduleExecutionSettings$databaseId,
    saveDirectory = resultsFolder,
    tablePrefix = moduleInfo$TablePrefix
  )
    
  
  # Export the results
  rlang::inform("Export data to csv files")

  sqliteFile <- file.path(resultsFolder,"sqliteCharacterization", "sqlite")
  if (file.exists(sqliteFile)) {
	  unlink(sqliteFile)
  }
  sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sqlite',
    server = sqliteFile
  )
    
  DescriptiveStudies::exportDatabaseToCsv(
    connectionDetails = sqliteConnectionDetails, 
    resultSchema = 'main', 
    targetDialect = 'sqlite', 
    tempEmulationSchema = NULL,
    tablePrefix = moduleInfo$TablePrefix,
    filePrefix = moduleInfo$TablePrefix,
    saveDirectory = resultsFolder
  )
  
  # Export the resultsDataModelSpecification.csv
  resultsDataModel <- CohortGenerator::readCsv(file = system.file("settings/resultsDataModelSpecification.csv",
                                                                  package = "DescriptiveStudies"),
                                               warnOnCaseMismatch = FALSE)
  newTableNames <- paste0(moduleInfo$TablePrefix, resultsDataModel$tableName)
  resultsDataModel$tableName <- newTableNames
  CohortGenerator::writeCsv(
    x = resultsDataModel,
    file = file.path(resultsFolder, "resultsDataModelSpecification.csv"),
    warnOnCaseMismatch = FALSE,
    warnOnFileNameCaseMismatch = FALSE,
    warnOnUploadRuleViolations = FALSE
  )  
  
  # Zip the results
  rlang::inform("Zipping csv files")
  DatabaseConnector::createZipFile(
    zipFile = file.path(resultsFolder, 'results.zip'),
    files = file.path(resultsFolder)
  )
}