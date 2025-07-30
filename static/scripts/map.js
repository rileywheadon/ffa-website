var map = L.map('map').setView([51.0447, -114.0719], 10);

L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
}).addTo(map);

// Add all stations to the map
fetch("/static/data/stations.json")
	.then(response => response.json())
	.then(data => {
		data.forEach(record => {

			// Get data from the JSON file
			const lat = parseFloat(record.LAT);
			const lon = parseFloat(record.LON);
			const name = record.STATION_NAME || "Unnamed";
			const number = record.STATION_NUMBER || "Missing";
			const label = `${name} (${number})`;

			// Add a marker to the map
			const marker = L.marker([lat, lon])
				.addTo(map)
				.bindTooltip(label, {
					permanent: false,
					direction: "top",
					offset: [-15, -10]
				});

			// Make an API request on click
			marker.on("click", function () {
				fetch("/dataset-geomet", {
					method: "POST",
					headers: {
						"Content-Type": "application/json"
					},
					body: JSON.stringify({ 
						number: number,
						name: name
					})  
			  	})
				.then(response => response.text())
				.then(html => {
					document.getElementById("01-sidebar").innerHTML = html;
				})
				.catch(err => {
					console.error("Failed to load station info:", err);
				});
			});

		});
  	})
  	.catch(error => console.error("Error loading JSON data:", error));




