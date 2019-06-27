$(
    function loop() {
	var con = new WebSocket('ws://' + window.location.host + '/ws');
	$('#status').text = 'Connecting';
	con.onopen = function {
	    $('#status').text = 'Connected ' + con.protocol;
	};
	con.onmessage = function(e) {
	    $('#stats').html = e.data;
	};
	con.onclose = loop;
    }
)
