# plumber.R
library(plumber)
library(ffaframework) 
library(glue)
library(ggplot2)
library(base64enc)
library(rmarkdown)
library(jsonlite)

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
	ffaframework:::submodule_05(
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

#* @post /model-assessment
#* @serializer unboxedJSON 
function(data, years, splits, structures, distributions, intervals, options) {
	ffaframework:::submodule_06(
		data,
		years,
		distributions,
		lapply(intervals, function(x) if (is.na(x)) NULL else x),
		options,
		splits,
		apply(structures, 1, as.list),
		NULL,
		TRUE
	) 
}

# TODO: Fix serialization issue
#* @post /report-html
function(report_params) {

	print(str(report_params))

	# Get the template
	template <- system.file("templates", "_master.Rmd", package = "ffaframework")

	# Create temporary directory for the report
	report_dir <- tempdir()
	img_dir <- paste0(report_dir, "/img")
	if (!dir.exists(img_dir)) dir.create(img_dir)

	# Add the title and img_dir to the report parameters
	report_params <- c(
		report_params,
		list(title = "Framework Report", img_dir = img_dir)
	)

	# Render the template
	rmarkdown::render(
		input         = template,
		params        = report_params,
		output_format = "html_document",
		output_dir    = report_dir,
		output_file   = "report",
		quiet         = FALSE
	)

	# Return the output file
	output_file <- paste0(report_dir, "/report.html")
	include_file(output_file)

}
