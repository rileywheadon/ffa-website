# plumber.R
library(plumber)
library(ffaframework) 
library(glue)
library(ggplot2)
library(base64enc)

# Helper function for converting images to base64
serialize_plot <- function(plot) {
	buffer <- tempfile(fileext = ".png")
	ggsave(buffer, plot = plot, height = 8, width = 10, dpi = 300)
	base64enc::dataURI(file = buffer, mime = "image/png")
}

#* @post /data-summary
#* @serializer unboxedJSON
function(data, years) {

    data <- as.numeric(unlist(data))
    years <- as.integer(unlist(years))

    list(
		year_min = min(years),
		year_max = max(years),
		data_min = min(data),
		data_median = median(data),
		data_max = max(data),
		data_missing = max(years) - min(years) - length(data) + 1 
	)
}

#* @post /change-point-detection
#* @serializer unboxedJSON
function(data, years) {

    data <- as.numeric(unlist(data))
    years <- as.integer(unlist(years))

	# Run the Pettitt and MKS tests
	pettitt <- eda_pettitt_test(data, years)
	mks <- eda_mks_test(data, years)

	# Generate and save the plots
	pettitt$plot <- serialize_plot(plot_pettitt_test(pettitt))
	mks$plot <- serialize_plot(plot_mks_test(mks))

	# Return the results as a list
	list(
		period = c(min(years), max(years)),
		items = list(pettitt = pettitt, mks = mks)
	)

}

#* @post /trend-detection
#* @serializer unboxedJSON 
trend_detection <- function(data, years, splits) {
	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	lapply(periods, function(period) trend_detection_helper(data, years, period))
}

trend_detection_helper <- function(data, years, period) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Define list for storing the results
	items <- list()

	# White (1): go to MW-MK (2) regardless of the result
	trend01 <- function() {
		items$white <<- eda_white_test(data, years)
		return (2)
	}

	# MW-MK (2): go to Sen's variance (3) if there is non-stationarity, MK test (5) if not.
	trend02 <- function() {
		mw <- data_mw_variability(data, years)
		items$mwmk <<- eda_mk_test(mw$std)
		if (items$white$reject || items$mwmk$reject) 3 else 5
	}

	# Sen's variance (3): go to Runs variance (4) regardless of the result
	trend03 <- function() {
		mw <- data_mw_variability(data, years)
		items$sens_variance <<- eda_sens_trend(mw$std, mw$year)
		plot <- plot_sens_trend(mw$std, mw$year, items$sens_variance)
		items$sens_variance$plot <<- serialize_plot(plot)
		return (4)	
	}

	# Runs variance (4): go to MK test (5) regardless of the results.
	trend04 <- function() {
		items$runs_variance <<- eda_runs_test(items$sens_variance)
		plot <- plot_runs_test(items$runs_variance)
		items$runs_variance$plot <<- serialize_plot(plot)
		return (5)	
	}

	# MK (5): go to Spearman (6) if there is a trend, end (NULL) if not.
	trend05 <- function() {
		items$mk <<- eda_mk_test(data)
		if (items$mk$reject) 6 else NULL
	} 

	# Spearman (6): go to BB-MK (7) if there is serial correlation, Sen's means (10) if not.
	trend06 <- function() {
		items$spearman <<- eda_spearman_test(data)
		plot <- plot_spearman_test(items$spearman)
		items$spearman$plot <<- serialize_plot(plot)
		if (items$spearman$reject) 7 else 10
	} 

	# BB-MK (7): go to PP (8) if there is a trend, end (NULL) if not.
	trend07 <- function() {
		items$bbmk <<- eda_bbmk_test(data)
		plot <- plot_bbmk_test(items$bbmk)
		items$bbmk$plot <<- serialize_plot(plot)
		if (items$bbmk$reject) 8 else NULL
	} 

	# PP (8): go to KPSS (9) regardless of the result
	trend08 <- function() {
		items$pp <<- eda_pp_test(data)
		return (9)
	}

	# KPSS (9): go to Sen's (10) regardless of the result
	trend09 <- function() {
		items$kpss <<- eda_kpss_test(data)
		return (10)
	}

	# Sen's means (10): go to Runs means (11) regardless of the result
	trend10 <- function() {
		items$sens_mean <<- eda_sens_trend(data, years)
		plot <- plot_sens_trend(data, years, items$sens_mean, items$sens_variance)
		items$sens_mean$plot <<- serialize_plot(plot)
		return (11)
	}

	# Runs means (11): go to end (NULL) regardless of the result
	trend11 <- function() {
		items$runs_mean <<- eda_runs_test(items$sens_mean)
		plot <- plot_runs_test(items$runs_mean)
		items$runs_mean$plot <<- serialize_plot(plot)
		return (NULL)
	}

	# Iterate through the flowchart
	location <- 1
	while (!is.null(location)) {
		fname <- sprintf("trend%02d", location)
		location <- get(fname)()
	} 

	# Return the results
	list(period = period, items = items)

}

#* @post /distribution-selection
#* @serializer unboxedJSON 
distribution_selection <- function(data, years, splits, structures) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { 
		distribution_selection_helper(data, years, periods[[i]], structures[[i]])
	})

}

distribution_selection_helper <- function(data, years, period, structure) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Compute the decomposed dataset
	decomposed <- data_decomposition(data, years, structure)

	# Run distribution selection
	selection <- select_ldistance(decomposed)

	# if (options$selection == "L-distance") {
	# 	select_ldistance(decomposed)
	# } else if (options$selection == "L-kurtosis") {
	# 	select_lkurtosis(decomposed)
	# } else if (options$selection == "Z-statistic") {
	# 	select_zstatistic(decomposed, options$z_samples)
	# } else {
	# 	list(method = "Preset", recommendation = options$selection)
	# }

	# Generate L-moments plot
	# if (options$selection %in% c("L-distance", "L-kurtosis", "Z-statistic")) {
	# 	pdf(nullfile())
	# 	plot <- plot_lmom_diagram(items$selection)
	# 	save_plot("selection", plot, period, img_dir)
	# }

	# Save the plot
	pdf(nullfile())
	selection$plot <- serialize_plot(plot_lmom_diagram(selection))

	# Return results as a list
	list(period = period, selection = selection)

}

#* @post /parameter-estimation
#* @serializer unboxedJSON 
parameter_estimation <- function(data, years, splits, structures, distributions) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { parameter_estimation_helper(
		data,
		years,
		periods[[i]],
		structures[[i]],
		distributions[[i]]
	)})

}

parameter_estimation_helper <- function(data, years, period, structure, distribution) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Run parameter estimation
	if (!structure$location && !structure$scale) {
		estimation <- fit_lmom_fast(data, distribution)
	} else {
		estimation <- fit_maximum_likelihood(data, distribution, NULL, years, structure)
	}

	estimation$distribution = distribution
	estimation$structure = structure

	# Return results as a list
	list(period = period, estimation = estimation)

}


#* @post /uncertainty-quantification
#* @serializer unboxedJSON 
uncertainty_quantification <- function(data, years, splits, structures, distributions) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { uncertainty_quantification_helper(
		data,
		years,
		periods[[i]],
		structures[[i]],
		distributions[[i]]
	)})

}

uncertainty_quantification_helper <- function(data, years, period, structure, distribution) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Run parameter estimation
	if (!structure$location && !structure$scale) {
		uncertainty <- uncertainty_bootstrap(
			data,
			distribution,
			"L-moments",
			years = years,
			structure = structure
		)
		uncertainty$plot = serialize_plot(plot_sffa(uncertainty))
	} else {
		uncertainty <- uncertainty_rfpl(
			data,
			distribution,
			years = years,
			structure = structure
		)
		uncertainty$plot = serialize_plot(plot_nsffa(uncertainty))
	}
	
	uncertainty$distribution = distribution
	uncertainty$structure = structure

	# Return results as a list
	list(period = period, uncertainty = uncertainty)

}
