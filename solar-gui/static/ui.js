function amp_seconds_to_amp_hours(as) {
    return as / 3600;
}

function jouls_to_kwh(j) {
    return j / 3600000;
}

function display_stats(stats) {
    if (stats.hasOwnProperty('V0')) {
	var stats = stats.V0;
	$('#timestamp').html(stats.timestamp);
	$('#battery_sense_voltage').html(stats.battery_sense_voltage);
	$('#charge_current').html(stats.charge_current);
	$('#array_power').html(stats.array_power);
	$('#charge_state').html(stats.charge_state);
	$('#load_state').html(stats.load_state);
	$('#ah_charge_daily').html(amp_seconds_to_amp_hours(stats.ah_charge_daily));
	$('#load_state').html(stats.load_state);
	$('#kwh_charge_total').html(jouls_to_kwh(stats.kwh_charge_total));
    } else {
	console.log("error, unexpected stats version " + stats);
    }
}

function loop() {
    var con = new WebSocket('ws://' + window.location.host + '/ws/');
    var iid = 0;

    $('#status').text('Connecting');
    con.onopen = function() {
	$('#status').text('Connected ' + con.protocol);
	iid = window.setInterval(function() { con.send('"StatsCurrent"') }, 5000);
    };
    con.onmessage = function(e) {
	var v = JSON.parse(e.data);
	if (v == 'CmdOk') { }
	else if (v.hasOwnProperty('CmdErr')) {
	    console.log(v);
	} else if (v.hasOwnProperty('Stats')) {
	    display_stats(v.Stats);
	} else if (v.hasOwnProperty('Status')) {
	    console.log(v);
	} else {
	    console.log("unknown response from server: " + v);
	}
    };
    con.onclose = function() {
	$('#status').text("Not Connected");
	window.clearInterval(iid);
	loop();
    };
    con.onerror = function(e) {
	$('#status').text("Disconnected " + e);
	window.clearInterval(iid);
	loop();
    };
};

loop();
