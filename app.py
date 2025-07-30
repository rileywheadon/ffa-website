from flask import Flask, request, render_template, redirect, jsonify, make_response, send_from_directory, Response, url_for
import pandas as pd
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


def get_or_create_uid():
    uid = request.cookies.get("uid")
    if not uid: uid = str(uuid.uuid4())
    return uid

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
        splits_raw = [item.strip() for item in splits.split(',')]
        splits_int = [int(item) for item in splits_raw]
    except ValueError:
        return False

    if any(x < min(years) or x > max(years) for x in splits_int):
        return False

    return splits_int


# Redirect to previous stage
@app.route("/view-previous-stage/<position>", methods = ["GET"])
def view_previous_stage(position):
    uid = get_or_create_uid()

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

    # Remove all subsequent stages from Redis
    for i in range(int(position), 9):
        remove_from_valkey(uid, routes[i])

    # Redirect to the correct route
    return redirect(url_for(routes[int(position) - 1]))


#############
# ENDPOINTS #
#############
 

@app.route("/", methods = ["GET"])
def index():
    uid = get_or_create_uid()
    response = make_response(render_template("00-index.html"))
    response.set_cookie("uid", uid, max_age = COOKIE_AGE, httponly = True)
    return response


@app.route("/start-analysis", methods = ["GET"])
def start_analysis():
    uid = get_or_create_uid()

    # Clear any previous analyses created by the user
    key = f"analysis:{uid}"
    if valkey.exists(key):
        valkey.delete(key)

    # Redirect the user to dataset selection
    return redirect(url_for("dataset_selection"))


@app.route("/dataset-selection", methods = ["GET"])
def dataset_selection():
    uid = get_or_create_uid()

    # Render the summary for the current dataset if it is stored in redis
    summary = read_from_valkey(uid, "data_summary")
    if summary:
        local = not ("station_name" in summary)
        print(local)
        return render_template("01-main.html", summary = summary, local = local)

    # Redirect the user to dataset selection
    return render_template("01-main.html", summary = None, local = False)


@app.route("/dataset-local", methods = ["POST"])
def dataset_local():

    uid = get_or_create_uid()

    # Check that the request contains a file
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']

    # Read CSV and validate columns
    try:
        stream = io.StringIO(file.stream.read().decode("utf-8"))
        df = pd.read_csv(stream, comment = "#")
        df = df[df["max"].notna()]
    except Exception as e:
        return jsonify({"error": "Failed to read CSV", "details": str(e)}), 400

    if not {"max", "year"}.issubset(df.columns):
        return jsonify({"error": "CSV must contain 'max' and 'year' columns"}), 400

    # Hit plumber endpoint to get summary information
    payload = {"data": df["max"].tolist(), "years": df["year"].tolist()}
    summary = access_r_api("data-summary", payload) 

    # Save data, years, and summary to valkey
    summary["file_name"] = file.filename
    payload["data_summary"] = summary
    write_to_valkey(uid, payload) 

    # Render the updated dataset selection endpoint
    return render_template("01-main.html", summary = summary, local = True)


@app.route("/dataset-geomet", methods = ["POST"])
def dataset_geomet():

    uid = get_or_create_uid()

    station_name = request.get_json().get("name")
    station_number = request.get_json().get("number")
    station_data = STATION_DATA.get(station_number)

    if station_data is None:
        abort(404, description=f"Station '{station_number}' not found.")

    # Hit plumber endpoint to get summary information
    payload = {"data": station_data["MAX"], "years": station_data["YEAR"]}
    summary = access_r_api("data-summary", payload) 

    # Save data, years, and summary to valkey.
    summary["station_name"] = station_name
    summary["station_number"] = station_number
    payload["data_summary"] = summary
    write_to_valkey(uid, payload)

    # Store as JSON strings in cookies (signed for safety)
    return render_template("01-sidebar-geomet.html", summary = summary)

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


@app.route("/change-point-detection", methods = ["GET"])
def change_point_detection():

    uid = get_or_create_uid()

    # If change point detection already exists in Redis, render it
    results = read_from_valkey(uid, "change_point_detection")
    if results:
        return render_template("02-main.html", results = results)

    # Hit plumber endpoint to get summary information
    data = read_from_valkey(uid, "data")
    years = read_from_valkey(uid, "years")
    request_data = {"data": data, "years": years}
    results = access_r_api("change-point-detection", request_data)
    write_to_valkey(uid, {"change_point_detection": results}) 

    # Return the template
    return render_template("02-main.html", results = results)


@app.route("/trend-detection", methods = ["GET", "POST"])
def trend_detection():

    uid = get_or_create_uid()

    # If change point detection already exists in Redis, render it
    results = read_from_valkey(uid, "trend_detection")
    if results:
        return render_template("03-main.html", results = results)

    # Check that split points are integers within the years of the data
    data = read_from_valkey(uid, "data")
    years = read_from_valkey(uid, "years")
    splits = validate_splits(request.form.get("points"), years)

    if splits is False:
        return jsonify(error = "error: Invalid split points"), 400

    # Hit plumber endpoint to get trend detection information
    request_data = {"data": data, "years": years, "splits": splits}
    results = access_r_api("trend-detection", request_data)
    write_to_valkey(uid, {"splits": splits, "trend_detection": results})

    # Render the template
    return render_template("03-main.html", results = results)

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

    return render_template("04-main.html", eda = eda)

@app.route("/distribution-selection", methods = ["POST"])
def distribution_selection():

    uid = get_or_create_uid()
    keys = request.form.keys()
    splits = read_from_valkey(uid, "splits")

    # If distribution selection results already exist in Redis, render it
    results = read_from_valkey(uid, "distribution_selection")
    if results:
        return render_template("05-main.html", results = results)

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
    data = read_from_valkey(uid, "data")
    years = read_from_valkey(uid, "years")
    payload = {"data": data, "years": years, "splits": splits, "structures": structures}
    results = access_r_api("distribution-selection", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"distribution_selection": results})
    return render_template("05-main.html", results = results)

@app.route("/parameter-estimation", methods = ["POST"])
def parameter_estimation():

    uid = get_or_create_uid()
    keys = request.form.keys()
    splits = read_from_valkey(uid, "splits")

    # If distribution selection results already exist in Redis, render it
    # results = read_from_valkey(uid, "parameter_estimation")
    # if results:
    #     return render_template("06-main.html", results = results)

    # List of valid distributions
    options = ["GEV", "NOR", "LNO", "GEV", "GLO", "GNO", "PE3", "LP3", "WEI"]

    # Create list of nonstationary structures
    distributions = []
    for i in range(len(splits) + 1):
        distribution = request.form.get(f"{i}-distribution")
        if distribution not in options: return "uh oh"
        distributions.append(distribution)
        
    # Save the nonstationary structures to valkey
    write_to_valkey(uid, {"distributions": distributions})

    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": splits,
        "structures": read_from_valkey(uid, "structures"),
        "distributions": distributions
    }

    results = access_r_api("parameter-estimation", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"parameter_estimation": results})
    return render_template("06-main.html", results = results)


@app.route("/uncertainty-quantification", methods = ["GET", "POST"])
def uncertainty_quantification():

    uid = get_or_create_uid()

    # Hit the R API
    payload = {
        "data": read_from_valkey(uid, "data"),
        "years": read_from_valkey(uid, "years"),
        "splits": read_from_valkey(uid, "splits"),
        "structures": read_from_valkey(uid, "structures"),
        "distributions": read_from_valkey(uid, "distributions")
    }

    results = access_r_api("uncertainty-quantification", payload)

    # Write the results to valkey, render the distribution selection template
    write_to_valkey(uid, {"uncertainty_quantification": results})
    return render_template("07-main.html", results = results)


if __name__ == "__main__":
    app.run(debug=True, port=5000)

