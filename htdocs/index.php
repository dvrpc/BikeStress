<!DOCTYPE html>
<!-- Handcrafted, locally sourced sustainable free-range HTML -->

<!-- 3rd Party -->

<html>
    <head>
        <title>?</title>
        <!-- Bootstrap CSS -->
        <link rel="stylesheet" href="css/bootstrap.min.css">
        <link rel="stylesheet" href="css/bootstrap-theme.min.css">

        <!-- Jquery UI CSS -->
        <link rel="stylesheet" href="css/jquery-ui.min.css">
        <link rel="stylesheet" href="css/jquery-ui.structure.min.css">
        <link rel="stylesheet" href="css/jquery-ui.theme.min.css">

        <!-- Leaflet CSS -->
        <link rel="stylesheet" href="css/leaflet.css" />

        <!-- ? -->
        <link rel="stylesheet" href="css/index.css">
        <style>
        .handle {
            height: 1em;
        }
        
        #gpano {
            position: absolute;
            height: 100%;
            width: 100%;
        }
        #gmap {
            float: right;
            height: 30%;
            width: 30%;
            z-index: 1;
        }
        </style>
    </head>
    <body>
        <!--
        <div id="box">
            <div id="left" class="col-md-3">&nbsp;</div>
            <div id="map" class="col-md-9"></div>
        </div>
        -->

        <div id="box">
            <div id="hud">
                <div id="temp"></div>
                <div class="draggable resizable">
                    <div class="handle"></div>
                    <div id="gpano"></div>
                    <div id="gmap"></div>
                </div>
            </div>
            <div id="map"></div>
        </div>

        <!-- JQuery -->
        <script src="js/lib/jquery-3.2.1.min.js"></script>
        <script src="js/lib/jquery-ui.min.js"></script>

        <!-- Bootstrap -->
        <script src="js/lib/bootstrap.min.js"></script>

        <!-- Leaflet -->
        <script src="js/lib/leaflet.js"></script>
        <script async defer src="https://maps.googleapis.com/maps/api/js?key=AIzaSyCPcULgKxlZHywXVke42fo1PVcd2So1GU8&callback=initialize"></script>

        <!-- Leaflet-easyPrint -->
        <!-- https://github.com/rowanwins/leaflet-easyPrint -->
        <script src="js/lib/leaflet-easyPrint.js"></script>

        <!-- ? -->
        <script src="js/index.js"></script>

    </body>
</html>




