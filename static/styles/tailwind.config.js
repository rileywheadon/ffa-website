const path = require("path");
const root = __dirname; 

module.exports = {
	content: [
		path.join(root, "../../templates/**"),
		path.join(root, "../scripts/**")
	],
	theme: { extend: {} },
	plugins: [],
}
