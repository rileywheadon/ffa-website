# plumber.R
library(plumber)
library(ffaframework) 
library(base64enc)
library(rmarkdown)
library(jsonlite)
library(glue)
library(ggplot2)


# Helper function for converting images to base64
serialize_plot <- function(plot) {
	buffer <- tempfile(fileext = ".png")
	ggsave(buffer, plot = plot, height = 8, width = 10, dpi = 300)
	base64enc::dataURI(file = buffer, mime = "image/png")
}

#* @get /health
function() {
  list(available = TRUE)
}

#* @post /dataset-selection
#* @serializer unboxedJSON
function(data, years) {
    data <- as.numeric(unlist(data))
    years <- as.integer(unlist(years))
	data_screening(data, years)
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
	ffaframework:::submodule_01(data, years, options, NULL, TRUE)
}

#* @post /trend-detection
#* @serializer unboxedJSON 
function(data, years, options, splits) {
	ffaframework:::submodule_02(data, years, options, splits, NULL, TRUE)
}

#* @post /distribution-selection
#* @serializer unboxedJSON 
function(data, years, splits, structures, options) {
	ffaframework:::submodule_03(
		data,
		years,
		options,
		splits,
		apply(structures, 1, as.list),
		NULL,
		TRUE
	)
}

#* @post /parameter-estimation
#* @serializer unboxedJSON 
function(data, years, splits, structures, distributions, options) {
	ffaframework:::submodule_04(
		data,
		years,
		distributions,
		options,
		splits,
		apply(structures, 1, as.list),
		NULL,
		TRUE
	)
}

#* @post /uncertainty-quantification
#* @serializer unboxedJSON 
function(data, years, splits, structures, distributions, options) {
	tryCatch(
		expr = { ffaframework:::submodule_05(
			data,
			years,
			distributions,
			options,
			splits,
			apply(structures, 1, as.list),
			NULL,
			TRUE
		)},
		error = function(e) {
			list(error = e$message)
		}
	)
}

#* @post /model-assessment
#* @serializer unboxedJSON 
function(data, years, splits, structures, distributions, intervals, options) {
	ffaframework:::submodule_06(
		data,
		years,
		distributions,
		lapply(intervals, function(x) if (is.data.frame(x)) x else NULL),
		options,
		splits,
		apply(structures, 1, as.list),
		NULL,
		TRUE
	) 
}

#* @post /report-html
#* @serializer contentType list(type = "text/html; charset=utf-8")
function(req, res) {

	raw <- if (!is.null(req$postBody)) req$postBody else rawToChar(req$body)

	results <- jsonlite::fromJSON(
		txt               = raw,
		simplifyVector    = TRUE,
		simplifyDataFrame = FALSE,
		simplifyMatrix    = FALSE
  	)$report_params

	# Now results is purely lists/vectors. Coerce Pettitt, MKS, and uncertainty to dataframe.
	results$submodule_results[[1]]$tests$pettitt$change_points <- {
		change_points <- results$submodule_results[[1]]$tests$pettitt$change_points
		do.call(rbind, lapply(change_points, as.data.frame))
	}

	results$submodule_results[[1]]$tests$mks$change_points <- {
		change_points <- results$submodule_results[[1]]$tests$mks$change_points
		do.call(rbind, lapply(change_points, as.data.frame))
	}

	for (i in seq_along(results$submodule_results)) {
	  	block <- results$submodule_results[[i]]

		if (block$name == "Uncertainty Quantification") {
			if ("ci" %in% names(block$uncertainty)) {
				results$submodule_results[[i]]$uncertainty$ci <-
					do.call(rbind, lapply(block$uncertainty$ci, as.data.frame))
				next
			}

			for (j in seq_along(block$uncertainty$ci_list)) {
				results$submodule_results[[i]]$uncertainty$ci_list[[j]] <-
					do.call(rbind, lapply(block$uncertainty$ci_list[[j]], as.data.frame))
			}
		}
	}

	# Create temporary directory for the report
	report_dir <- tempdir()
	img_dir <- paste0(report_dir, "/img")
	if (!dir.exists(img_dir)) dir.create(img_dir)

	# Add the title and img_dir to the report parameters
	report_params <- c(
		results,
		list(title = "Framework Report", img_dir = img_dir)
	)

	# Render the template
	rmarkdown::render(
		input         = system.file("templates", "_master.Rmd", package = "ffaframework"),
		params        = report_params,
		output_format = "html_document",
		output_dir    = report_dir,
		output_file   = "report",
		quiet         = TRUE
	)

	# Return the output file
	output_file <- paste0(report_dir, "/report.html")
	readChar(output_file, file.info(output_file)$size, useBytes = TRUE)

}
