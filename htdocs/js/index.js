var map,
    printPlugin;

var paperUSLetterP = {
    width: 850,
    height: 1100,
    className: 'A4Portrait page',
    name: 'Portrait'
}
var paperUSLetterL = {
    width: 1100,
    height: 840,
    className: 'A4Landscape page',
    name: 'Landscape'
}

function main() {
    var tileLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
    });
    tileLayer.__will__save = true;

    var _tl0 = L.tileLayer('https://arcgis.dvrpc.org/arcgis/rest/services/AppData/AssignedLTS/MapServer/tile/{z}/{y}/{x}', {
        attribution: "Sean Lawrence"
    });

    map = L.map('map', {
        center: [39.9522, -75.1639],
        zoom: 16
    })
    .addLayer(tileLayer)
    .addLayer(_tl0);
    
    printPlugin = L.easyPrint({
        title: 'Print',
        tileWait: 1000,
        position: 'topleft',
        sizeModes: [paperUSLetterP, paperUSLetterL]
    }).addTo(map);
}

main();