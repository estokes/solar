function amp_seconds_to_amp_hours(as) {
    return as / 3600;
}

function jouls_to_kwh(j) {
    return j / 3600000;
}

function loop() {
    var con = new WebSocket('ws://' + window.location.host + '/ws/');
    $('#status').text('Connecting');
    con.onopen = function() {
	$('#status').text('Connected ' + con.protocol);
    };
    con.onmessage = function(e) {
	var stats = JSON.parse(e.data);
	$('#timestamp').html(stats.timestamp);
	$('#battery_sense_voltage').html(stats.battery_sense_voltage);
	$('#charge_current').html(stats.charge_current);
	$('#array_power').html(stats.array_power);
	$('#charge_state').html(stats.charge_state);
	$('#load_state').html(stats.load_state);
	$('#ah_charge_daily').html(amp_seconds_to_amp_hours(stats.ah_charge_daily));
	$('#load_state').html(stats.load_state);
	$('#kwh_charge_total').html(jouls_to_kwh(stats.kwh_charge_total))
    };
    con.onclose = function() {
	$('#status').text("Not Connected");
	loop();
    };
    con.onerror = function(e) {
	$('#status').text("Disconnected " + e);
	loop();
    };
};

loop();
