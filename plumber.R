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

#* @post /dataset-selection
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

#* @post /view-plot
#* @serializer unboxedJSON
function(data, years, title) {
    data <- as.numeric(unlist(data))
    years <- as.integer(unlist(years))
	title <- as.character(title)
	serialize_plot(plot_ams_data(data, years, title = title))
}

#* @post /change-point-detection
#* @serializer unboxedJSON
function(data, years, options) {

    data <- as.numeric(unlist(data))
    years <- as.integer(unlist(years))

	# Run the Pettitt and MKS tests
	pettitt <- eda_pettitt_test(data, years, options$significance_level)
	mks <- eda_mks_test(data, years, options$significance_level)

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
trend_detection <- function(data, years, splits, options) {
	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	lapply(periods, function(period) trend_detection_helper(data, years, period, options))
}

trend_detection_helper <- function(data, years, period, options) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Define list for storing the results
	items <- list()

	# White (1): go to MW-MK (2) regardless of the result
	trend01 <- function() {
		items$white <<- eda_white_test(data, years, options$significance_level)
		return (2)
	}

	# MW-MK (2): go to Sen's variance (3) if there is non-stationarity, MK test (5) if not.
	trend02 <- function() {
		mw <- data_mw_variability(data, years)
		items$mwmk <<- eda_mk_test(mw$std, options$significance_level)
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
		items$runs_variance <<- eda_runs_test(items$sens_variance, options$significance_level)
		plot <- plot_runs_test(items$runs_variance)
		items$runs_variance$plot <<- serialize_plot(plot)
		return (5)	
	}

	# MK (5): go to Spearman (6) if there is a trend, end (NULL) if not.
	trend05 <- function() {
		items$mk <<- eda_mk_test(data, options$significance_level)
		if (items$mk$reject) 6 else NULL
	} 

	# Spearman (6): go to BB-MK (7) if there is serial correlation, Sen's means (10) if not.
	trend06 <- function() {
		items$spearman <<- eda_spearman_test(data, options$significance_level)
		plot <- plot_spearman_test(items$spearman)
		items$spearman$plot <<- serialize_plot(plot)
		if (items$spearman$reject) 7 else 10
	} 

	# BB-MK (7): go to PP (8) if there is a trend, end (NULL) if not.
	trend07 <- function() {
		items$bbmk <<- eda_bbmk_test(data, options$significance_level, options$bbmk_samples)
		plot <- plot_bbmk_test(items$bbmk)
		items$bbmk$plot <<- serialize_plot(plot)
		if (items$bbmk$reject) 8 else NULL
	} 

	# PP (8): go to KPSS (9) regardless of the result
	trend08 <- function() {
		items$pp <<- eda_pp_test(data, options$significance_level)
		return (9)
	}

	# KPSS (9): go to Sen's (10) regardless of the result
	trend09 <- function() {
		items$kpss <<- eda_kpss_test(data, options$significance_level)
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
		items$runs_mean <<- eda_runs_test(items$sens_mean, options$significance_level)
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
distribution_selection <- function(
	data,
	years,
	splits,
	structures,
	options
) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { 
		distribution_selection_helper(
			data,
			years,
			periods[[i]],
			structures[[i]],
			options
		)
	})

}

distribution_selection_helper <- function(
	data,
	years,
	period,
	structure,
	options
) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Compute the decomposed dataset
	decomposed <- data_decomposition(data, years, structure)

	# Run distribution selection
	selection <- if (options$selection_method == "L-distance") {
		select_ldistance(decomposed)
	} else if (options$selection == "L-kurtosis") {
		select_lkurtosis(decomposed)
	} else if (options$selection == "Z-statistic") {
		select_zstatistic(decomposed, options$z_samples)
	} 

	# Save the plot
	pdf(nullfile())
	selection$plot <- serialize_plot(plot_lmom_diagram(selection))

	# Return results as a list
	list(period = period, selection = selection)

}

#* @post /parameter-estimation
#* @serializer unboxedJSON 
parameter_estimation <- function(
	data,
	years,
	splits,
	structures,
	distributions,
	options
) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { 
		parameter_estimation_helper(
			data,
			years,
			periods[[i]],
			structures[[i]],
			distributions[[i]],
			options
		)
	})

}

parameter_estimation_helper <- function(
	data,
	years,
	period,
	structure,
	distribution,
	options
) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Run parameter estimation
	estimation_method <- if (!structure$location && !structure$scale) {
		options$s_estimation
	} else {
		options$ns_estimation
	}

	estimation <- if (estimation_method == "L-moments") {
		fit_lmom_fast(data, distribution)
	} else if (estimation_method == "MLE") {
		fit_maximum_likelihood(data, distribution, NULL, years, structure)
	} else {
		fit_maximum_likelihood(data, distribution, options$gev_prior, years, structure)
	}

	estimation$distribution = distribution
	estimation$structure = structure

	# Return results as a list
	list(period = period, estimation = estimation)

}


#* @post /uncertainty-quantification
#* @serializer unboxedJSON 
uncertainty_quantification <- function(
	data,
	years,
	splits,
	structures,
	distributions,
	options
) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)

	lapply(seq_along(periods), function(i) { 
		uncertainty_quantification_helper(
			data,
			years,
			periods[[i]],
			structures[[i]],
			distributions[[i]],
			options
		)
	})

}

uncertainty_quantification_helper <- function(
	data,
	years,
	period,
	structure,
	distribution,
	options
) {

	# Subset data and years based on period
	idx <- which(years >= period[1] & years <= period[2])
	data <- data[idx]
	years <- years[idx]

	# Set nonstationary slices and uncertainty quantification method
	if (!structure$location && !structure$scale) {
		slices <- 1900
		uncertainty_method <- options$s_uncertainty
		estimation_method <- options$s_estimation
	} else {
		slices <- options$slices
		slices <- slices[slices >= period[1] & slices <= period[2]]
		uncertainty_method <- options$ns_uncertainty
		estimation_method <- options$ns_estimation
	}

	# Run uncertainty quantification 
	uncertainty <- if (uncertainty_method == "Bootstrap") {
		uncertainty_bootstrap(
			data,
			distribution,
			estimation_method,
			prior = options$gev_prior,
			years = years,
			structure = structure,
			slices = slices,
			alpha = options$significance_level,
			samples = options$bootstrap_samples,
			periods = options$return_periods
		)
	} else {
		uncertainty_rfpl(
			data,
			distribution,
			prior = if (uncertainty_method == "RFPL") NULL else options$gev_prior,
			years = years,
			structure = structure,
			slices = slices,
			alpha = options$significance_level,
			eps = options$rfpl_tolerance,
			periods = options$return_periods
		)
	}

	# Generate uncertainty quantificaiton plot
	uncertainty$plot <- if (!structure$location && !structure$scale) {
		serialize_plot(plot_sffa(uncertainty))
	} else {
		serialize_plot(plot_nsffa(uncertainty))
	}
	
	uncertainty$distribution = distribution
	uncertainty$structure = structure

	# Return results as a list
	list(period = period, uncertainty = uncertainty)

}

#* @post /model-assessment
#* @serializer unboxedJSON 
model_assessment <- function(
	data,
	years,
	splits,
	structures,
	distributions,
	estimation_list,
	uncertainty_list,
	options
) {

	starts <- c(min(years), as.numeric(splits))
	ends <- c(as.numeric(splits) - 1, max(years))
	periods <- Map(c, starts, ends)
	structures <- apply(structures, 1, as.list)
	estimation_list <- apply(estimation_list, 1, as.list)
	print(estimation_list)

	lapply(seq_along(periods), function(i) { 
		assessment <- model_diagnostics(
    		data,
    		distributions[[i]],
    		estimation_list[i, "estimation.params"],
    		uncertainty_list[[i]],
    		years = years,
    		structure = structures[[i]],
    		alpha = options$significance_level,
    		pp_formula = options$pp_formula
		)

		if (!structures[[i]]$location && !structures[[i]]$scale) {
			assessment$plot <- serialize_plot(plot_model_diagnostics(diagnostics))
		}

		list (period = period, assessment = assessment)
	})

}
