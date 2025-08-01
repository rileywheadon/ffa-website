from flask import Flask, request, render_template, redirect, jsonify, make_response, send_from_directory, Response, url_for
import pandas as pd
import base64
import redis
import requests
import uuid
import os
import io
import json

app = Flask(__name__)

# Connect to Valkey
valkey = redis.Redis(host='localhost', port=6379)


#############
# CONSTANTS #
#############


# Cookie age (1 week)
COOKIE_AGE = 60 * 60 * 24 * 7

# Data sources and constants
PLUMBER_URL = "http://localhost:8000"

# Load the entire JSON data once at startup
DATA_PATH = os.path.join(os.path.dirname(__file__), "static", "data", "statistics.json")
with open(DATA_PATH, "r", encoding="utf-8") as f:
    STATION_DATA = json.load(f)


####################
# HELPER FUNCTIONS #
####################


def write_to_valkey(uid, mapping):
    serialized_mapping = { k: json.dumps(v) for k, v in mapping.items() }
    valkey.hset(f"analysis:{uid}", mapping = serialized_mapping)


def remove_from_valkey(uid, field):
    valkey.hdel(f"analysis:{uid}", field)


def read_from_valkey(uid, field):
    try:
        return json.loads(valkey.hget(f"analysis:{uid}", field))
    except: 
        return None


def get_or_create_uid():
    uid = request.cookies.get("uid")
    if not uid: uid = str(uuid.uuid4())
    return uid


def get_or_create_options():
    uid = get_or_create_uid()
    options = read_from_valkey(uid, "options")

    # Return the already existing options
    if options is not None: 
        return options

    # Initialize a new options object, save to Valkey
    options = {
        "significance_level": 0.05,
        "bbmk_samples": 10000,
        "selection_method": "L-distance",
        "z_samples": 10000,
        "s_estimation": "L-moments",
        "ns_estimation": "MLE",
        "gev_prior": [6, 9],
        "s_uncertainty": "Bootstrap",
        "ns_uncertainty": "RFPL",
        "return_periods": [2, 5, 10, 20, 50, 100],
        "bootstrap_samples": 1000,
        "rfpl_tolerance": 0.1,
        "pp_formula": "Weibull"
    }

    write_to_valkey(uid, {"options": options})
    return options


def access_r_api(url, data):
    try:
        URL = f"{PLUMBER_URL}/{url}"
        response = requests.post(URL, json=data)
        return response.json()
    except requests.RequestException as e:
        return jsonify({"error": "Failed to contact R API", "details": str(e)}), 502


def validate_splits(splits, years):
    if splits == "": 
        return []

    try:
        splits = [int(item.strip()) for item in splits.split(',')]
    except ValueError:
        return False

    if any(x < min(years) or x > max(years) for x in splits):
        return False

    return splits


def module_handler(endpoint, repeat):
    uid = get_or_create_uid()

    # List of route functions for FFA modules
    routes = [
        "dataset_selection",
        "change_point_detection",
        "trend_detection",
        "approach_selection",
        "distribution_selection",
        "parameter_estimation",
        "uncertainty_quantification",
        "model_assessment",
        "report_generation"
    ]

    # Delete subsequent stages from Redis, including the current stage if repeat is True
    position = routes.index(endpoint)
    for i in range(position + int(not repeat), 9):
        remove_from_valkey(uid, routes[i])

    # Check if results for current endpoint are stored in Redis
    return read_from_valkey(uid, endpoint)


#############
# ENDPOINTS #
#############
 

@app.route("/", methods = ["GET"])
def index():
    uid = get_or_create_uid()
    response = make_response(render_template("index.html"))
    response.set_cookie("uid", uid, max_age = COOKIE_AGE, httponly = True)
    return response


@app.route("/edit-options", methods = ["GET"])
def edit_options():
    options = get_or_create_options()
    return render_template("options_modal.html", options = options)


@app.route("/save-options", methods = ["GET", "POST"])
def save_options():
    uid = get_or_create_uid()
    options = request.form.to_dict()

    payload = {
        "significance_level": float(options["significance_level"]),
        "bbmk_samples": int(options["bbmk_samples"]),
        "selection_method": options["selection_method"],
        "z_samples": int(options["z_samples"]),
        "s_estimation": options["s_estimation"],
        "ns_estimation": options["ns_estimation"],
        "gev_prior": list(map(int, options["gev_prior"].split(","))),
        "s_uncertainty": options["s_uncertainty"],
        "ns_uncertainty": options["ns_uncertainty"],
        "return_periods": list(map(int, options["return_periods"].split(","))),
        "bootstrap_samples": int(options["bootstrap_samples"]),
        "rfpl_tolerance": float(options["rfpl_tolerance"]),
        "pp_formula": options["pp_formula"]
    }

    write_to_valkey(uid, {"options": payload})
    return redirect(request.referrer)


@app.route("/dataset-selection", methods = ["GET"])
def dataset_selection():
    uid = get_or_create_uid()

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    results = module_handler("dataset_selection", repeat)
    return render_template("modules/dataset_selection.html", results = results)


@app.route("/dataset-local", methods = ["POST"])
def dataset_local():
    uid = get_or_create_uid()

    # Check that the request contains a file
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    # Read CSV and validate columns
    try:
        file = request.files['file']
        stream = io.StringIO(file.stream.read().decode("utf-8"))
        df = pd.read_csv(stream, comment = "#")
        df = df[df["max"].notna()]
    except Exception as e:
        return jsonify({"error": "Failed to read CSV", "details": str(e)}), 400

    if not {"max", "year"}.issubset(df.columns):
        return jsonify({"error": "CSV must contain 'max' and 'year' columns"}), 400

    # Hit plumber endpoint to get summary information
    data = df["max"].tolist()
    years = df["year"].tolist()
    results = access_r_api("dataset-selection", {"data": data, "years": years}) 

    # Save data, years, and summary to valkey
    results["file_name"] = file.filename
    write_to_valkey(uid, {"data": data, "years": years, "dataset_selection": results}) 

    # Render the dataset selection endpoint
    return redirect(url_for("dataset_selection")) 


@app.route("/dataset-geomet/<station_number>/<station_name>", methods = ["GET"])
def dataset_geomet(station_number, station_name):

    uid = get_or_create_uid()
    station_data = STATION_DATA.get(station_number)

    if station_data is None:
        abort(404, description=f"Station '{station_number}' not found.")

    # Hit plumber endpoint to get summary information
    data = station_data["MAX"]
    years = station_data["YEAR"]
    results = access_r_api("dataset-selection", {"data": data, "years": years}) 

    # Save data, years, and summary to valkey.
    results["station_name"] = station_name
    results["station_number"] = station_number
    write_to_valkey(uid, {"data": data, "years": years, "dataset_selection": results})

    # Render the dataset selection endpoint
    return redirect(url_for("dataset_selection")) 


@app.route("/download-geomet/<station_number>", methods = ["GET"])
def download_geomet(station_number):

    # Get the data into a pandas DataFrame
    station_data = STATION_DATA.get(station_number)
    df = pd.DataFrame(station_data)

    # Write CSV to in-memory buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Create downloadable response
    filename = f"{station_number}.csv"
    return Response(
        buffer,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/view-plot", methods = ["GET"])
def view_plot():

    uid = get_or_create_uid()
    results = read_from_valkey(uid, "dataset_selection")

    if "file_name" in results:
        title = results["file_name"]
    else:
        title = f"{results['station_name']} ({results['station_number']})"

    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "title": title
    }

    plot = access_r_api("view-plot", payload) 
    return render_template("plot_modal.html", results = results, plot = plot)


@app.route("/download-plot", methods = ["GET"])
def download_plot():

    uid = get_or_create_uid()
    results = read_from_valkey(uid, "dataset_selection")

    if "file_name" in results:
        title = results["file_name"]
        filename = results["file_name"].split(".")[0]
    else:
        title = f"{results['station_name']} ({results['station_number']})"
        filename = results["station_number"]

    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "title": title
    }

    plot = access_r_api("view-plot", payload) 
    plot = plot.split(",", 1)[1]
    buffer = base64.b64decode(plot)

    return Response(
        buffer,
        mimetype="image/png",
        headers={"Content-Disposition": f"attachment; filename={filename}.png"}
    )


@app.route("/change-point-detection", methods = ["GET"])
def change_point_detection():
    uid = get_or_create_uid()
    template = "modules/change_point_detection.html"

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("change_point_detection", repeat)

    if results is not None:
        return render_template(template, results = results, options = options)

    # Hit plumber endpoint to get summary information
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "options": options
    }

    results = access_r_api("change-point-detection", payload)
    write_to_valkey(uid, {"change_point_detection": results}) 

    # Return the template
    return render_template(template, results = results, options = options)


@app.route("/trend-detection", methods = ["GET", "POST"])
def trend_detection():
    uid = get_or_create_uid()
    template = "modules/trend_detection.html"

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("trend_detection", repeat)

    if results is not None:
        return render_template(template, results = results, options = options)

    # Check that split points are integers within the years of the data
    years = read_from_valkey(uid, "years")

    if request.form.get("splits") is not None:
        splits = validate_splits(request.form.get("splits"), years)
    else:
        splits = read_from_valkey(uid, "splits")

    if splits is False:
        return jsonify(error = "error: Invalid split points"), 400

    # Hit plumber endpoint to get trend detection information
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": years,
        "splits": splits,
        "options": options
    }

    results = access_r_api("trend-detection", payload)
    write_to_valkey(uid, {"splits": splits, "trend_detection": results})

    # Render the template
    return render_template(template, results = results, options = options)


@app.route("/approach-selection", methods = ["GET", "POST"])
def approach_selection():
    uid = get_or_create_uid()

    if request.method == "POST":

        years = read_from_valkey(uid, "years")
        splits = validate_splits(request.form.get("splits"), years)

        if splits is False: 
            return jsonify(error = "error: Invalid split points"), 400

        write_to_valkey(uid, {"splits": splits})

    eda = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": read_from_valkey(uid, "splits"),
        "change_point_detection": read_from_valkey(uid, "change_point_detection"),
        "trend_detection": read_from_valkey(uid, "trend_detection"),
    }

    return render_template("modules/approach_selection.html", eda = eda)

@app.route("/distribution-selection", methods = ["GET", "POST"])
def distribution_selection():
    uid = get_or_create_uid()
    template = "modules/distribution_selection.html"

    keys = request.form.keys()
    splits = read_from_valkey(uid, "splits")

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("distribution_selection", repeat)

    if results is not None:
        return render_template(template, results = results, options = options)

    # Create list of nonstationary structures
    structures = []
    for i in range(len(splits) + 1):
        structures.append({
            "location": f"{i}-location" in keys,
            "scale": f"{i}-scale" in keys,
        })

    # Save the nonstationary structures to valkey
    write_to_valkey(uid, {"structures": structures})

    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": splits,
        "structures": structures,
        "options": options
    }

    results = access_r_api("distribution-selection", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"distribution_selection": results})
    return render_template(template, results = results, options = options)


@app.route("/parameter-estimation", methods = ["GET", "POST"])
def parameter_estimation():
    uid = get_or_create_uid()
    template = "modules/parameter_estimation.html"

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("parameter_estimation", repeat)

    if results is not None:
        return render_template(template, results = results, options = options)
    
    # Create or load the list of distributions
    splits = read_from_valkey(uid, "splits")
    distributions = read_from_valkey(uid, "distributions")

    if not distributions:
        for i in range(len(splits) + 1):
            distribution = request.form.get(f"{i}-distribution")
            distributions.append(distribution)
        
        write_to_valkey(uid, {"distributions": distributions})

    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": splits,
        "structures": read_from_valkey(uid, "structures"),
        "distributions": distributions,
        "options": options
    }

    results = access_r_api("parameter-estimation", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"parameter_estimation": results})
    return render_template(template, results = results, options = options)


@app.route("/uncertainty-quantification", methods = ["GET", "POST"])
def uncertainty_quantification():
    uid = get_or_create_uid()
    template = "modules/uncertainty_quantification.html"

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("uncertainty_quantification", repeat)

    # if results is not None:
    #     return render_template(template, results = results, options = options)
    
    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": read_from_valkey(uid, "splits"),
        "structures": read_from_valkey(uid, "structures"),
        "distributions": read_from_valkey(uid, "distributions"),
        "options": options
    }

    print(options)

    results = access_r_api("uncertainty-quantification", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"uncertainty_quantification": results})
    return render_template(template, results = results, options = options)


@app.route("/model-assessment", methods = ["GET", "POST"])
def model_assessment():
    uid = get_or_create_uid()
    template = "modules/model_assessment.html"

    # If results for this module are cached, use them
    repeat = request.args.get("repeat")
    options = get_or_create_options()
    results = module_handler("model_assessment", repeat)

    # if results is not None:
    #     return render_template(template, results = results, options = options)
    print(read_from_valkey(uid, "parameter_estimation"))

    
    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": read_from_valkey(uid, "splits"),
        "structures": read_from_valkey(uid, "structures"),
        "distributions": read_from_valkey(uid, "distributions"),
        "estimation_list": read_from_valkey(uid, "parameter_estimation"),
        "uncertainty_list": read_from_valkey(uid, "uncertainty_quantification"),
        "options": options
    }

    results = access_r_api("model-assessment", payload)
    print(results)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"model_assessment": results})
    return render_template(template, results = results, options = options)


@app.route("/report-generation", methods = ["GET"])
def report_generation():
    return render_template("modules/report_generation.html")


if __name__ == "__main__":
    app.run(debug=True, port=5000)

