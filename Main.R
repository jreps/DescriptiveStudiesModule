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
  
  workFolder <- jobContext$moduleExecutionSettings$workSubFolder # does this exist?
 
  rlang::inform("Executing DescriptiveStudies")
  
  # run the models
  DescriptiveStudies::runCharacterizationAnalyses(
    connectionDetails = jobContext$moduleExecutionSettings$connectionDetails, 
    targetDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    targetTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
    outcomeDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema,
    outcomeTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable,
    cdmDatabaseSchema = jobContext$moduleExecutionSettings$cdmDatabaseSchema, 
    characterizationSettings = jobContext$settings, 
    databaseId = jobContext$moduleExecutionSettings$databaseId, # where to get this?
    saveDirectory = workFolder
    #tempEmulationSchema = 
  )
    
  
  # Export the results
  rlang::inform("Export data to csv files")

  sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sqlite',
    server = file.path(workFolder,"sqliteCharacterization", "sqlite")
  )
    

  DescriptiveStudies::exportDatabaseToCsv(
    connectionDetails = sqliteConnectionDetails, 
    resultSchema = 'main', 
    stringAppendToTables = '',
    targetDialect = 'sqlite', 
    tempEmulationSchema = NULL,
    saveDirectory = file.path(workFolder, 'results')
  )
  
  resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
  
  # Zip the results
  rlang::inform("Zipping csv files")
  OhdsiSharing::compressFolder(
    sourceFolder = file.path(workFolder, 'results'), 
    targetFileName = file.path(resultsFolder, 'results.zip')
  )
  
  resultsDataModel <- CohortGenerator::readCsv(
    file = system.file(
      "settings", "resultsDataModelSpecification.csv", 
       package = "DescriptiveStudies")
    )
  CohortGenerator::writeCsv(
    x = resultsDataModel, 
    file = file.path(resultFolder, "resultsDataModelSpecification.csv"),
    warnOnFileNameCaseMismatch = FALSE
    )
  
}