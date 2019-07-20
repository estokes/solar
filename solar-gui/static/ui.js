function amp_seconds_to_amp_hours(as) {
    return as / 3600;
}

function jouls_to_kwh(j) {
    return j / 3600000;
}

function set_rg_status(obj, state) {
    if(state) {
	$(obj).addClass('green');
	$(obj).removeClass('red');
    } else {
	$(obj).addClass('red');
	$(obj).removeClass('green');
    }
}

function display_stats(stats) {
    if (stats.hasOwnProperty('V1')) {
	var phy = stats.V1.phy;
	var stats = stats.V1.controller;
	$('#timestamp').html(stats.timestamp);
	$('#battery_sense_voltage').html(stats.battery_sense_voltage.toFixed(2));
	$('#charge_current').html(stats.charge_current.toFixed(2));
	$('#array_power').html(stats.array_power.toFixed(2));
	$('#charge_state').html(stats.charge_state);
	$('#load_state').html(stats.load_state);
	$('#ah_charge_daily').html(amp_seconds_to_amp_hours(stats.ah_charge_daily).toFixed(2));
	$('#load_state').html(stats.load_state);
	$('#kwh_charge_total').html(jouls_to_kwh(stats.kwh_charge_total.toFixed(2)));
	set_rg_status($('#timestamp'), Date.now() - new Date(stats.timestamp) < 60000);
	set_rg_status(
	    $('#load_status'),
	    stats.load_state == 'Normal' || stats.load_state == 'LVD'
	);
	set_rg_status(
	    $('#charging_status'),
	    stats.charge_state == 'BulkMPPT'
		|| stats.charge_state == 'Float'
		|| stats.charge_state == 'Night'
		|| stats.charge_state == "Absorption"
	);
	set_rg_status($('#phy_solar'), phy.solar);
	set_rg_status($('#phy_battery'), phy.battery);
	set_rg_status($('#phy_master'), phy.master);
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
		backgroundColor: 'rgb(0,170,200)',
		borderColor: 'rgb(0,170,200)',
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

function trim(data) { while(data.length > 1440) { data.shift(); } }

function update_charts(stats) {
    var chargeCurrent = window.chartChargeCurrentCfg.data.datasets[0].data;
    var ahCharge = window.chartAhChargeCfg.data.datasets[0].data;
    var batteryVoltage = window.chartBatteryVoltageCfg.data.datasets[0].data;
    var arrayPower = window.chartArrayPowerCfg.data.datasets[0].data;
    stats.forEach(e => {
	var ts = new Date(e.V1.controller.timestamp);
	chargeCurrent.push({
	    x: ts,
	    y: e.V1.controller.charge_current
	});
	ahCharge.push({
	    x: ts,
	    y: amp_seconds_to_amp_hours(e.V1.controller.ah_charge_daily)
	});
	batteryVoltage.push({
	    x: ts,
	    y: e.V1.controller.battery_sense_voltage
	});
	arrayPower.push({
	    x: ts,
	    y: e.V1.controller.array_power
	});
    })
    trim(chargeCurrent);
    trim(ahCharge);
    trim(batteryVoltage);
    trim(arrayPower);
    window.chartChargeCurrent.update();
    window.chartAhCharge.update();
    window.chartBatteryVoltage.update();
    window.chartArrayPower.update();
}

function init_charts() {
    window.chartChargeCurrentCfg = empty_chart_config('Charge Current', 'Amps');
    window.chartChargeCurrent = new Chart($('#current_chart'), window.chartChargeCurrentCfg);
    window.chartAhChargeCfg = empty_chart_config('Ah Charge', 'Ah');
    window.chartAhCharge = new Chart($('#ah_chart'), window.chartAhChargeCfg);
    window.chartBatteryVoltageCfg = empty_chart_config('Battery Voltage', 'Volts');
    window.chartBatteryVoltage = new Chart($('#battery_voltage_chart'), window.chartBatteryVoltageCfg);
    window.chartArrayPowerCfg = empty_chart_config('Array Power', 'Watts');
    window.chartArrayPower = new Chart($('#array_power_chart'), window.chartArrayPowerCfg);
}

var con = null;

function set(tgt, v) {
    if(con != null) {
	con.send(`{"Set": ["${tgt}", ${v}]}`);
    }
}

function loop() {
    var receiving_history = false;
    var history = [];
    var stats_iid = 0;
    var chart_iid = 0;
    var last = Date.now();

    var reload = function(msg) {
	$('#status').text(msg);
	console.log("connection was closed");
	window.clearInterval(stats_iid);
	window.clearInterval(chart_iid);
	con = null;
	loop();
    };

    if(con != null) return;
    else con = new WebSocket('ws://' + window.location.host + '/ws/');
    
    $('#status').text('Connecting');
    con.onopen = function() {
	$('#status').text('Loading History');
	receiving_history = true;
	con.send('{"StatsHistory": 10}');
	init_charts();
	stats_iid = window.setInterval(() => { if(!receiving_history) con.send('"StatsCurrent"'); }, 5000);
	chart_iid = window.setInterval(() => { if(!receiving_history) con.send('"StatsDecimated"'); }, 540000);
    };
    con.onmessage = function(e) {
	var now = Date.now();
	if(now - last <= 600000) { last = now; }
	else {
	    con.close();
	    return;
	}
	var v = JSON.parse(e.data);
	if (v == 'CmdOk') { }
	else if (v.hasOwnProperty('CmdErr')) {
	    console.log(v);
	} else if (v.hasOwnProperty('Stats')) {
	    history.push(v.Stats);
	    if(!receiving_history) display_stats(v.Stats);
	} else if (v.hasOwnProperty('StatsDecimated')) {
	    if(!receiving_history) update_charts([v.StatsDecimated]);
	} else if (v.hasOwnProperty('Status')) {
	    console.log(v);
	} else if (v == 'EndOfHistory') {
	    $('#status').text('Connected');
	    receiving_history = false;
	    update_charts(history);
	    history = [];
	} else {
	    console.log("unknown response from server: " + v);
	}
    };
    
    con.onclose = function() { reload("Disconnected"); }
    con.onerror = function(e) {
	reload("Disconnected " + e);
    };
};

window.onload = function() {
    loop();
    $('#enable_load').click(function() { set('Load', true); });
    $('#disable_load').click(function() { set('Load', false); });
    $('#enable_charging').click(function() { set('Charging', true); });
    $('#disable_charging').click(function() { set('Charging', false); });
    $('#enable_phy_solar').click(function() { set('PhySolar', true) });
    $('#disable_phy_solar').click(function() { set('PhySolar', false) });
    $('#enable_phy_battery').click(function() { set('PhyBattery', true) });
    $('#disable_phy_battery').click(function() { set('PhyBattery', false) });
    $('#enable_phy_master').click(function() { set('PhyMaster', true) });
    $('#disable_phy_master').click(function() { set('PhyMaster', false) });
}
