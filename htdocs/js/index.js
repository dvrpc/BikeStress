
function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });
    tileLayer.__will__save = true;

    map = L.map('map', {
        center: [39.9522, -75.1639],
        zoom: 16
    }).addLayer(tileLayer);
}


main();