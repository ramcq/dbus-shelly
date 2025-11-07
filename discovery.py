#!/usr/bin/python3

from __future__ import annotations
import sys
import os
import asyncio
import re
from functools import partial

# aiovelib
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'ext', 'aiovelib'))
from aiovelib.service import Service, IntegerItem, TextItem
from aiovelib.localsettings import Setting
from aiovelib.client import Monitor

try:
	from dbus_fast.aio import MessageBus
except ImportError:
	from dbus_next.aio import MessageBus

# Import local modules first
from shelly_device import ShellyDevice
from utils import logger, wait_for_settings

# Try to import zeroconf, but make it optional
ZEROCONF_AVAILABLE = True
try:
	from zeroconf import ServiceStateChange
	from zeroconf.asyncio import (
		AsyncServiceBrowser,
		AsyncServiceInfo,
		AsyncZeroconf
	)
except ImportError:
	logger.warning("zeroconf module not available, will use MQTT discovery only")
	ZEROCONF_AVAILABLE = False

# Try to import paho-mqtt from Venus OS paths
MQTT_AVAILABLE = False
for mqtt_path in ['/opt/victronenergy/dbus-mqtt', '/opt/victronenergy/mqtt-rpc']:
	if os.path.exists(mqtt_path) and mqtt_path not in sys.path:
		sys.path.insert(1, mqtt_path)

try:
	import paho.mqtt.client as mqtt
	MQTT_AVAILABLE = True
	logger.info("paho-mqtt available, MQTT discovery enabled")
except ImportError:
	logger.warning("paho-mqtt not available, MQTT discovery disabled")

background_tasks = set()

class ShellyDiscovery(object):
	def __init__(self, bus_type):
		self.discovered_devices = []
		self.saved_devices = []
		self.shellies = {}
		self.service = None
		self.settings = None
		self.bus_type = bus_type
		self.aiobrowser = None
		self.aiozc = None
		self._mdns_lock = asyncio.Lock()
		self._shelly_lock = asyncio.Lock()
		self._enable_tasks = {}
		# MQTT discovery
		self.mqtt_client = None
		self._mqtt_lock = asyncio.Lock()
		self._mqtt_pending_queries = {}  # Maps request_id to asyncio.Future
		self._mqtt_request_id = 0

	async def start(self):
		# Connect to dbus, localsettings
		self.bus = await MessageBus(bus_type=self.bus_type).connect()
		self.monitor = await Monitor.create(self.bus, itemsChanged=self.items_changed)

		self.settings = await wait_for_settings(self.bus)

		# Set up the service
		self.service = Service(self.bus, "com.victronenergy.shelly")
		await self.settings.add_settings(Setting('/Settings/Shelly/IpAddresses', "", alias="ipaddresses"))
		await self.settings.add_settings(Setting('/Settings/Shelly/MqttBroker', "localhost", alias="mqttbroker"))

		ip_addresses = self.settings.get_value(self.settings.alias('ipaddresses'))
		mqtt_broker = self.settings.get_value(self.settings.alias('mqttbroker'))

		self.service.add_item(IntegerItem('/Refresh', 0, writeable=True,
			onchange=self.refresh))
		self.service.add_item(TextItem('/IpAddresses', ip_addresses, writeable=True, onchange=self._on_ip_addresses_changed))
		self.service.add_item(TextItem('/MqttBroker', mqtt_broker, writeable=True, onchange=self._on_mqtt_broker_changed))
		await self.service.register()

		# Start zeroconf discovery if available
		if ZEROCONF_AVAILABLE:
			try:
				self.aiozc = AsyncZeroconf()
				self.aiobrowser = AsyncServiceBrowser(
					self.aiozc.zeroconf, ["_shelly._tcp.local."], handlers=[self.on_service_state_change]
				)
				logger.info("mDNS discovery started")
			except Exception as e:
				logger.error("Failed to start mDNS discovery: %s", e)
		else:
			logger.info("mDNS discovery not available")

		# Start MQTT discovery if available
		if MQTT_AVAILABLE:
			await self._start_mqtt_discovery(mqtt_broker)
		else:
			logger.info("MQTT discovery not available")

		self._start_add_by_ip_address_task(ip_addresses)

		await self.bus.wait_for_disconnect()

	def _start_add_by_ip_address_task(self, ip_addresses):
		task = asyncio.create_task(self._add_devices_by_ip(ip_addresses.split(',')))
		background_tasks.add(task)
		task.add_done_callback(background_tasks.discard)

	async def _on_ip_addresses_changed(self, item, value):
		if value == "":
			if self.settings.get_value(self.settings.alias('ipaddresses')) != "":
				await self.settings.set_value(self.settings.alias('ipaddresses'), value)
			item.set_local_value(value)
			return

		for ip in value.split(','):
			# Validate IP address format
			rgx = re.compile(
				r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
			)
			if not rgx.match(ip):
				logger.error("Invalid IP address format: %s", ip)
				return
		if value != self.settings.get_value(self.settings.alias('ipaddresses')):
			await self.settings.set_value(self.settings.alias('ipaddresses'), value)
		item.set_local_value(value)

		self._start_add_by_ip_address_task(value)

	async def _add_devices_by_ip(self, ipaddresses):
		for serial in self.saved_devices:
			device_ip = self.service.get_item('/Devices/{}/Ip'.format(serial))

			# Remove manually added devices that are no longer in the list, but only if they are not currently enabled.
			if device_ip is not None and device_ip.value not in ipaddresses and serial not in self.shellies:
				await self.stop_shelly_device(serial)
				self.remove_discovered_device(serial)

		for ip in set(ipaddresses): # Use set to avoid duplicates
			ip_found = False
			# Check if we already have this device
			for serial in self.discovered_devices + self.saved_devices:
				device_ip = self.service.get_item('/Devices/{}/Ip'.format(serial))
				if device_ip is not None and device_ip.value == ip:
					logger.info("Device with IP %s already found, SN: %s", ip, serial)
					ip_found = True
					break

			if not ip_found:
				await self._add_device(ip, serial=None, manual=True)

	async def refresh(self, item, value):
		if value == 1:
			# Delete discovered devices that are currently disabled.
			for serial in self.discovered_devices + self.saved_devices:
				delete = True
				i = 1
				while (True):
					enabled_item = self.service.get_item(f'/Devices/{serial}/{i}/Enabled')
					if enabled_item is None:
						break
					# Only delete if all channels are disabled
					if enabled_item.value == 1:
						delete = False
						break
					i += 1
				if delete:
					# Remove from the list if not enabled
					self.remove_discovered_device(serial)
				elif serial in self.shellies:
					# Try reconnecting if enabled.
					self.shellies[serial]['device'].do_reconnect()

			# Restart mDNS discovery if available
			if ZEROCONF_AVAILABLE and self.aiozc is not None:
				async with self._mdns_lock:
					if self.aiobrowser is not None:
						await self.aiobrowser.async_cancel()
					self.aiobrowser = AsyncServiceBrowser(
						self.aiozc.zeroconf, ["_shelly._tcp.local."], handlers=[self.on_service_state_change]
					)

			# Retry adding the manually added IP addresses
			self._start_add_by_ip_address_task(self.settings.get_value(self.settings.alias('ipaddresses')))
		item.set_local_value(0)

	async def _on_mqtt_broker_changed(self, item, value):
		"""Handle MQTT broker setting changes."""
		if not MQTT_AVAILABLE:
			return

		if value != self.settings.get_value(self.settings.alias('mqttbroker')):
			await self.settings.set_value(self.settings.alias('mqttbroker'), value)
		item.set_local_value(value)

		# Restart MQTT discovery with new broker
		if self.mqtt_client is not None:
			self.mqtt_client.disconnect()
		await self._start_mqtt_discovery(value)

	async def _start_mqtt_discovery(self, broker):
		"""Initialize MQTT client and start discovery."""
		if not MQTT_AVAILABLE:
			return

		try:
			import json
			self._json = json

			# Create MQTT client
			self.mqtt_client = mqtt.Client(client_id="dbus-shelly-discovery", protocol=mqtt.MQTTv311)
			self.mqtt_client.on_connect = self._mqtt_on_connect
			self.mqtt_client.on_message = self._mqtt_on_message

			# Connect to broker
			logger.info("Connecting to MQTT broker at %s", broker)
			self.mqtt_client.connect_async(broker, 1883, 60)
			self.mqtt_client.loop_start()

		except Exception as e:
			logger.error("Failed to start MQTT discovery: %s", e)

	def _mqtt_on_connect(self, client, userdata, flags, rc):
		"""Called when MQTT client connects to broker."""
		if rc == 0:
			logger.info("Connected to MQTT broker")
			# Subscribe to wildcard topic to discover all Shelly devices
			# Gen2+ devices publish to <device_id>/online when they connect
			client.subscribe("+/online", qos=0)
			# Also subscribe to our RPC response topic
			client.subscribe("dbus-shelly-discovery/rpc", qos=0)
			logger.info("Subscribed to +/online and dbus-shelly-discovery/rpc")
		else:
			logger.error("Failed to connect to MQTT broker, rc=%d", rc)

	def _mqtt_on_message(self, client, userdata, msg):
		"""Called when MQTT message is received."""
		try:
			topic = msg.topic
			payload = msg.payload.decode('utf-8')

			# Handle device online messages
			if topic.endswith('/online'):
				device_id = topic[:-7]  # Remove '/online' suffix
				if payload.lower() == 'true':
					# Device is online, query it for details
					task = asyncio.create_task(self._mqtt_handle_online(device_id))
					background_tasks.add(task)
					task.add_done_callback(background_tasks.discard)

			# Handle RPC responses
			elif topic == "dbus-shelly-discovery/rpc":
				try:
					data = self._json.loads(payload)
					task = asyncio.create_task(self._mqtt_handle_rpc_response(data))
					background_tasks.add(task)
					task.add_done_callback(background_tasks.discard)
				except Exception as e:
					logger.error("Failed to parse RPC response: %s", e)

		except Exception as e:
			logger.error("Error handling MQTT message: %s", e)

	async def _mqtt_handle_online(self, device_id):
		"""Handle device online announcement."""
		try:
			# Check if device ID matches Shelly pattern
			# Gen2 devices: shellyplus*/shellypro* followed by model and MAC
			if not (device_id.startswith('shellyplus') or device_id.startswith('shellypro')):
				return

			# Extract MAC address from device ID (last 12 characters)
			mac = device_id.split('-')[-1] if '-' in device_id else None
			if not mac or len(mac) != 12:
				return

			# Check if we already discovered this device
			if mac in self.discovered_devices or mac in self.saved_devices:
				logger.debug("Device %s already discovered", device_id)
				return

			logger.info("Discovered new Shelly device via MQTT: %s", device_id)

			# Query device for details via MQTT RPC
			ip = await self._mqtt_query_device(device_id, "Shelly.GetStatus")

			if ip:
				# Add device with the discovered IP
				await self._add_device(ip, serial=mac, manual=False, discovery_type='MQTT')

		except Exception as e:
			logger.error("Error handling device online for %s: %s", device_id, e)

	async def _mqtt_query_device(self, device_id, method, params=None):
		"""Query a Shelly device via MQTT RPC and return IP address if available."""
		if not MQTT_AVAILABLE or self.mqtt_client is None:
			return None

		try:
			# Generate unique request ID
			self._mqtt_request_id += 1
			request_id = self._mqtt_request_id

			# Create RPC request
			rpc_request = {
				"id": request_id,
				"src": "dbus-shelly-discovery",
				"method": method
			}
			if params:
				rpc_request["params"] = params

			# Create future to wait for response
			future = asyncio.Future()
			self._mqtt_pending_queries[request_id] = future

			# Publish request
			topic = f"{device_id}/rpc"
			payload = self._json.dumps(rpc_request)
			self.mqtt_client.publish(topic, payload, qos=0)

			logger.debug("Sent MQTT RPC request to %s: %s", device_id, method)

			# Wait for response with timeout
			try:
				response = await asyncio.wait_for(future, timeout=5.0)

				# Extract IP address from response
				# Response should have eth.ip or wifi.ip in the result
				if response and 'result' in response:
					result = response['result']
					# Try ethernet first
					if 'eth' in result and result['eth'] and 'ip' in result['eth']:
						return result['eth']['ip']
					# Try wifi
					if 'wifi' in result and result['wifi'] and 'ip' in result['wifi']:
						return result['wifi']['ip']
					# Try sys component (some devices)
					if 'sys' in result and 'available_updates' in result:
						# This is a full status, look for network info
						for key in result:
							if isinstance(result[key], dict):
								if 'ip' in result[key] and result[key]['ip']:
									return result[key]['ip']

			except asyncio.TimeoutError:
				logger.warning("Timeout waiting for MQTT RPC response from %s", device_id)
			finally:
				# Clean up pending query
				self._mqtt_pending_queries.pop(request_id, None)

		except Exception as e:
			logger.error("Error querying device %s via MQTT: %s", device_id, e)

		return None

	async def _mqtt_handle_rpc_response(self, data):
		"""Handle RPC response from device."""
		try:
			request_id = data.get('id')
			if request_id in self._mqtt_pending_queries:
				future = self._mqtt_pending_queries.get(request_id)
				if future and not future.done():
					future.set_result(data)
		except Exception as e:
			logger.error("Error handling RPC response: %s", e)

	def remove_discovered_device(self, serial):
		if serial in self.discovered_devices:
			self.discovered_devices.remove(serial)
		elif serial in self.saved_devices:
			self.saved_devices.remove(serial)
		else:
			return
		with self.service as s:
			s['/Devices/{}/Ip'.format(serial)] = None
			s['/Devices/{}/Mac'.format(serial)] = None
			s['/Devices/{}/Model'.format(serial)] = None
			s['/Devices/{}/Name'.format(serial)] = None
			s['/Devices/{}/DiscoveryType'.format(serial)] = None
			i = 1
			while self.service.get_item(key := f'/Devices/{serial}/{i}/Enabled') is not None:
				s[key] = None
				i += 1

	async def add_shelly_device(self, serial, server):
		event = asyncio.Event()
		s = ShellyDevice(
			bus_type=self.bus_type,
			serial=serial,
			server=server,
			event=event
		)

		e = asyncio.create_task(
			self._shelly_event_monitor(event, s)
		)
		try:
			await s.start()
		except Exception as e:
			logger.error("Failed to start shelly device %s: %s", serial, e)
			await s.stop()
			return
		e.add_done_callback(partial(self.delete_shelly_device, serial))
		self.shellies[serial] = {'device': s, 'event_mon': e}

	async def enable_shelly_channel(self, serial, channel, server):
		""" Enable a shelly channel. """

		# Sync device creation to prevent creating a device multiple times
		# when enabling multiple channels at once.
		async with self._shelly_lock:
			if serial not in self.shellies:
				await self.add_shelly_device(serial, server)

		return await self.shellies[serial]['device'].start_channel(channel)

	def delete_shelly_device(self, serial, fut=None):
		if serial in self.shellies:
			del self.shellies[serial]

	async def disable_shelly_channel(self, serial, channel):
		""" Disable a shelly channel. """
		if serial not in self.shellies:
			return False
		await self.shellies[serial]['device'].stop_channel(channel)

		if len(self.shellies[serial]['device'].active_channels) == 0:
			logger.info("No active channels left for device %s, stopping device", serial)
			await self.shellies[serial]['device'].stop()
		return True

	async def stop_shelly_device(self, serial):
		if serial in self.shellies:
			await self.shellies[serial]['device'].stop()
		else:
			logger.warning("Device not found: %s", serial)

	def items_changed(self, service, values):
		pass

	async def stop(self):
		# Stop MQTT discovery if active
		if MQTT_AVAILABLE and self.mqtt_client is not None:
			try:
				self.mqtt_client.loop_stop()
				self.mqtt_client.disconnect()
			except Exception as e:
				logger.error("Error stopping MQTT client: %s", e)

		# Stop zeroconf discovery if active
		if ZEROCONF_AVAILABLE:
			if self.aiobrowser is not None:
				await self.aiobrowser.async_cancel()
			if self.aiozc is not None:
				await self.aiozc.async_close()

	async def _shelly_event_monitor(self, event, shelly):
		serial = shelly.serial
		try:
			while True:
				await event.wait()
				event.clear()
				e = shelly.event

				if e == "disconnected":
					logger.warning("Shelly device %s disconnected", serial)
					await self.stop_shelly_device(serial)
					self.remove_discovered_device(serial)
					return

				elif e == "stopped":
					return

				event.clear()
		except asyncio.CancelledError:
			logger.info("Shelly event monitor for %s cancelled", serial)
		return

	async def _get_device_info(self, server, serial=None):
		ip = None
		info = None
		num_channels = 0
		# Only server info is needed for obtaining device info
		shelly = ShellyDevice(
			server=server,
			serial=serial
		)

		try:
			if not await shelly.connect():
				raise Exception()

			if len (shelly.get_capabilities()) == 0:
				logger.warning("Unsupported shelly device: %s", server)
				raise Exception()

			if not shelly._shelly_device or not shelly._shelly_device.connected:
				logger.error("Failed to connect to shelly device %s", server)
				raise Exception()

			info = await shelly.get_device_info()
			ip = shelly.server

			# Report shelly energy meter as device with one channel, so it shows up once in the UI
			num_channels = len(await shelly.get_channels()) if shelly.has_switch else 1

		except:
			pass
		finally:
			await shelly.stop()
			del shelly
		return ip, info, num_channels

	def on_service_state_change(self, zeroconf, service_type, name, state_change):
		rgx = re.compile(
			r"^shelly[\d\w\-]+-[0-9a-f]{12}\._shelly\._tcp\.local\.$"
		)

		if rgx.match(name):
			task = asyncio.get_event_loop().create_task(self._on_service_state_change_async(zeroconf, service_type, name, state_change))
			task.add_done_callback(background_tasks.discard)
			background_tasks.add(task)

	async def _add_device(self, server, serial=None, manual=False, discovery_type=None):
		ip, device_info, num_channels = await self._get_device_info(server, serial)
		if device_info is None:
			logger.error("Failed to get device info for %s", server)
			return

		if serial is None:
			serial = device_info.get('mac', 'unknown').replace(":", "")

		if manual:
			self.saved_devices.append(serial)
		else:
			self.discovered_devices.append(serial)

		# Shelly plus plug S example: 'app': 'PlusPlugS', 'model': 'SNPL-00112EU'
		model_name = device_info.get('app', device_info.get('model', 'Unknown'))
		# Custom name of the shelly device, if available
		name = device_info.get('name', None)

		for p in ['Ip', 'Mac', 'Model', 'Name', 'DiscoveryType']:
			if self.service.get_item('/Devices/{}/{}'.format(serial, p)) is None:
				self.service.add_item(TextItem('/Devices/{}/{}'.format(serial, p), writeable=False))

		# Determine discovery type
		if discovery_type is None:
			discovery_type = 'Manual' if manual else 'mDNS'

		with self.service as s:
			s['/Devices/{}/Ip'.format(serial)] = ip
			s['/Devices/{}/Mac'.format(serial)] = serial
			s['/Devices/{}/Model'.format(serial)] = model_name
			s['/Devices/{}/Name'.format(serial)] = name
			s['/Devices/{}/DiscoveryType'.format(serial)] = discovery_type

		for i in range(num_channels):
			await self.settings.add_settings(Setting('/Settings/Devices/shelly_{}/{}/Enabled'.format(serial, i + 1), 0, alias="enabled_{}_{}".format(serial, i)))
			enabled = self.settings.get_value(self.settings.alias('enabled_{}_{}'.format(serial, i)))

			if self.service.get_item('/Devices/{}/{}/Enabled'.format(serial, i + 1)) is None:
				enabled_item = IntegerItem('/Devices/{}/{}/Enabled'.format(serial, i + 1), writeable=True, onchange=partial(self._on_enabled_changed, serial, i))
				self.service.add_item(enabled_item)
			else:
				enabled_item = self.service.get_item('/Devices/{}/{}/Enabled'.format(serial, i + 1))

			with self.service as s:
				s['/Devices/{}/{}/Enabled'.format(serial, i + 1)] = enabled

			if enabled:
				await self._on_enabled_changed(serial, i, enabled_item, enabled)

	async def _on_service_state_change_async(self, zeroconf, service_type, name, state_change):
		async with self._mdns_lock:
			info = AsyncServiceInfo(service_type, name)
			await info.async_request(zeroconf, 3000)
			if not info or not info.server:
				return
			serial = info.server.split(".")[0].split("-")[-1]

			if (state_change == ServiceStateChange.Added or state_change == ServiceStateChange.Updated) and serial not in self.discovered_devices + self.saved_devices:
				logger.info("Found shelly device: %s", serial)
				await self._add_device(info.server[:-1], serial)

			elif state_change == ServiceStateChange.Removed and serial in self.discovered_devices:
				logger.warning("Shelly device: %s disappeared", serial)
				if serial in self.shellies:
					self.shellies[serial]['device'].do_reconnect()

	async def _on_enabled_changed(self, serial, channel, item, value):
		if value not in (0, 1) or item.service is None:
			return

		if value == 1:
			server = self.service['/Devices/{}/Ip'.format(serial)]
			# Start enabling a channel as a task, so multiple channels can be enabled simultaneously.
			task = asyncio.create_task(self.enable_shelly_channel(serial, channel, server))
			# Keep track of enabling tasks per device, so we can wait for them to finish when disabling channels.
			if serial not in self._enable_tasks:
				self._enable_tasks[serial] = set()
			self._enable_tasks[serial].add(task)
			task.add_done_callback(self._enable_tasks[serial].discard)
			ret = True
		else:
			if serial in self._enable_tasks:
				# Wait for any ongoing enable task on this device to finish before disabling
				await asyncio.gather(*self._enable_tasks[serial])
				self._enable_tasks[serial].clear()
			ret = await self.disable_shelly_channel(serial, channel)

		if ret:
			item.set_local_value(value)
			await self.settings.set_value(self.settings.alias('enabled_{}_{}'.format(serial, channel)), value)
