createDescriptiveStudiesModuleSpecifications <- function(
  targetIds, 
  outcomeIds,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 30,
  riskWindowStart = 1, 
  startAnchor = 'cohort start',
  riskWindowEnd = 0,
  endAnchor = 'cohort end', 
  covariateSettings = FeatureExtraction::createDefaultCovariateSettings()
) {
  #analysis <- list()
  #for (name in names(formals(createCohortDiagnosticsModuleSpecifications))) {
  #  analysis[[name]] <- get(name)
  #}
  
  timeToEventSettings <- DescriptiveStudies::createTimeToEventSettings(
    targetIds = targetIds, 
    outcomeIds = outcomeIds
    )
  
  dechallengeRechallengeSettings <- DescriptiveStudies::createDechallengeRechallengeSettings(
    targetIds = targetIds, 
    outcomeIds = outcomeIds, 
    dechallengeStopInterval = dechallengeStopInterval, 
    dechallengeEvaluationWindow = dechallengeEvaluationWindow
  )
  
  aggregateCovariateSettings <- DescriptiveStudies::createAggregateCovariateSettings(
    targetIds = targetIds, 
    outcomeIds = outcomeIds,
    riskWindowStart = riskWindowStart, 
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor, 
    covariateSettings = covariateSettings
      )
  
  analysis <- DescriptiveStudies::createCharacterizationSettings(
    timeToEventSettings = list(timeToEventSettings), 
    dechallengeRechallengeSettings = list(dechallengeRechallengeSettings), 
    aggregateCovariateSettings = list(aggregateCovariateSettings)
    )
  
  specifications <- list(module = "DescriptiveStudiesModule",
                         version = "0.0.1",
                         remoteRepo = "github.com",
                         remoteUsername = "jreps",
                         settings = analysis)
  class(specifications) <- c("DescriptiveStudiesModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}