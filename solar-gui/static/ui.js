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

function empty_chart_config(label, unit) {
    return {
	type: 'line',
	data: {
	    labels: [],
	    datasets: [{
		label: label,
		backgroundColor: 'rgb(255,99,132)',
		borderColor: 'rgb(255,99,132)',
		data: [],
		fill: false,
	    }]
	},
	options: {
	    responsive: true,
	    title: {
		display: true,
		text: label
	    },
	    tooltips: {
		mode: 'index',
		intersect: false,
	    },
	    hover: {
		mode: 'nearest',
		intersect: true
	    },
	    scales: {
		xAxes: [{
		    type: 'time',
		    time: {
			tooltipFormat: 'll YYYY-MM-DD HH:mm'
		    },
		    display: true,
		    scaleLabel: {
			display: true,
			labelString: 'Time'
		    }
		}],
		yAxes: [{
		    display: true,
		    scaleLabel: {
			display: true,
			labelString: unit
		    }
		}]
	    }
	}
    }
}

function update_charts(stats) {
    stats.forEach(e => {
	var ts = new Date(e.V0.timestamp);
	window.chartChargeCurrentCfg.data.datasets[0].data.push({
	    x: ts,
	    y: e.V0.charge_current
	})
	window.chartAhChargeCfg.data.datasets[0].data.push({
	    x: ts,
	    y: amp_seconds_to_amp_hours(e.V0.ah_charge_daily)
	})
    })
    window.chartChargeCurrent.update();
    window.chartAhCharge.update();
}

function init_charts() {
    window.chartChargeCurrentCfg = empty_chart_config('Charge Current', 'Amps');
    window.chartChargeCurrent = new Chart($('#current_chart'), window.chartChargeCurrentCfg);
    window.chartAhChargeCfg = empty_chart_config('Ah Charge', "Ah");
    window.chartAhCharge = new Chart($('#ah_chart'), window.chartAhChargeCfg);
}

var con = null;

function loop() {
    var receiving_history = false;
    var history = [];
    var iid = 0;

    if(con != null) return;
    else con = new WebSocket('ws://' + window.location.host + '/ws/');
    
    $('#status').text('Connecting');
    con.onopen = function() {
	$('#status').text('Connected ' + con.protocol);
	receiving_history = true;
	con.send('{"StatsHistory": 3}');
	iid = window.setInterval(() => { if(!receiving_history) con.send('"StatsCurrent"'); }, 5000);
	init_charts();
    };
    con.onmessage = function(e) {
	var v = JSON.parse(e.data);
	if (v == 'CmdOk') { }
	else if (v.hasOwnProperty('CmdErr')) {
	    console.log(v);
	} else if (v.hasOwnProperty('Stats')) {
	    if(receiving_history) { history.push(v.Stats); }
	    else {
		display_stats(v.Stats);
		update_charts([v.Stats]);
	    }
	} else if (v.hasOwnProperty('Status')) {
	    console.log(v);
	} else if (v == 'EndOfHistory') {
	    receiving_history = false;
	    update_charts(history);
	} else {
	    console.log("unknown response from server: " + v);
	}
    };
    con.onclose = function() {
	$('#status').text("Not Connected");
	console.log("connection was closed");
	window.clearInterval(iid);
	con = null;
	loop();
    };
    con.onerror = function(e) {
	$('#status').text("Disconnected " + e);
	console.log("connection was closed " + e);
	window.clearInterval(iid);
	con = null;
	loop();
    };
};

window.onload = loop();
