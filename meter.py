import asyncio
import logging
from asyncio.exceptions import TimeoutError # Deprecated in 3.11

try:
	from dbus_fast.aio import MessageBus
except ImportError:
	from dbus_next.aio import MessageBus

from __main__ import VERSION
from __main__ import __file__ as MAIN_FILE

from aiovelib.service import Service, IntegerItem, DoubleItem, TextItem
from aiovelib.service import TextArrayItem
from aiovelib.client import Monitor, ServiceHandler
from aiovelib.localsettings import SettingsService, Setting, SETTINGS_SERVICE

logger = logging.getLogger(__name__)

class LocalSettings(SettingsService, ServiceHandler):
	pass
		
# Text formatters
unit_watt = lambda v: "{:.0f}W".format(v)
unit_volt = lambda v: "{:.1f}V".format(v)
unit_amp = lambda v: "{:.1f}A".format(v)
unit_kwh = lambda v: "{:.2f}kWh".format(v)
unit_productid = lambda v: "0x{:X}".format(v)

class Meter(object):
	def __init__(self, bus_type):
		self.bus_type = bus_type
		self.monitor = None
		self.service = None
		self.custom_name = None
		self.position = None
		self.destroyed = False
		self.settings_paths = {}

	async def wait_for_settings(self):
		""" Attempt a connection to localsettings. If it does not show
		    up within 5 seconds, return None. """
		try:
			return await asyncio.wait_for(
				self.monitor.wait_for_service(SETTINGS_SERVICE), 5)
		except TimeoutError:
			pass

		return None
	
	def get_settings(self):
		""" Non-async version of the above. Return the settings object
		    if known. Otherwise return None. """
		return self.monitor.get_service(SETTINGS_SERVICE)

	async def start(self, host, port, data):
		try:
			mac = data['result']['mac']
			fw = data['result']['fw_id']
		except KeyError:
			return False

		# Connect to dbus, localsettings
		bus = await MessageBus(bus_type=self.bus_type).connect()
		self.monitor = await Monitor.create(bus, self.settings_changed)

		settingprefix = '/Settings/Devices/shelly_' + mac
		logger.info(f"Using settings prefix: {settingprefix}")

		# Store paths for later reference
		self.settings_paths = {
			"instance": f"{settingprefix}/ClassAndVrmInstance",
			"position": f"{settingprefix}/Position",
			"customname": f"{settingprefix}/CustomName"
		}

		logger.info("Waiting for localsettings")
		settings = await self.wait_for_settings()
		if settings is None:
			logger.error("Failed to connect to localsettings")
			return False

		logger.info("Connected to localsettings")

		await settings.add_settings(
			Setting(self.settings_paths["instance"], f"grid:40", 0, 0),
			Setting(self.settings_paths["position"], 0, 0, 2),
			Setting(self.settings_paths["customname"], "", 0, 0)
		)

		# Determine role and instance
		role, instance = self.role_instance(
			settings.get_value(self.settings_paths["instance"]))

		# Set up the service
		self.service = Service(bus, "com.victronenergy.{}.shelly_{}".format(role, mac))

		self.service.add_item(TextItem('/Mgmt/ProcessName', MAIN_FILE))
		self.service.add_item(TextItem('/Mgmt/ProcessVersion', VERSION))
		self.service.add_item(TextItem('/Mgmt/Connection', f"WebSocket {host}:{port}"))
		self.service.add_item(IntegerItem('/DeviceInstance', instance))
		self.service.add_item(IntegerItem('/ProductId', 0xB034, text=unit_productid))
		self.service.add_item(TextItem('/ProductName', "Shelly energy meter"))
		self.service.add_item(TextItem('/FirmwareVersion', fw))
		self.service.add_item(IntegerItem('/Connected', 1))
		self.service.add_item(IntegerItem('/RefreshTime', 100))

		# Get custom name from settings
		self.custom_name = settings.get_value(self.settings_paths["customname"])
		self.service.add_item(TextItem('/CustomName', self.custom_name or "",
			writeable=True, onchange=self.custom_name_changed))

		# Role
		self.service.add_item(TextArrayItem('/AllowedRoles',
			['grid', 'pvinverter', 'genset', 'acload']))
		self.service.add_item(TextItem('/Role', role, writeable=True,
			onchange=self.role_changed))

		# Position for pvinverter
		self.position = settings.get_value(self.settings_paths["position"])
		if role == 'pvinverter':
			self.service.add_item(IntegerItem('/Position',
				self.position,
				writeable=True, onchange=self.position_changed))

		# Indicate when we're masquerading for another device
		if role != "grid":
			self.service.add_item(IntegerItem('/IsGenericEnergyMeter', 1))

		# Meter paths
		self.service.add_item(DoubleItem('/Ac/Energy/Forward', None, text=unit_kwh))
		self.service.add_item(DoubleItem('/Ac/Energy/Reverse', None, text=unit_kwh))
		self.service.add_item(DoubleItem('/Ac/Power', None, text=unit_watt))
		for prefix in (f"/Ac/L{x}" for x in range(1, 4)):
			self.service.add_item(DoubleItem(prefix + '/Voltage', None, text=unit_volt))
			self.service.add_item(DoubleItem(prefix + '/Current', None, text=unit_amp))
			self.service.add_item(DoubleItem(prefix + '/Power', None, text=unit_watt))
			self.service.add_item(DoubleItem(prefix + '/Energy/Forward', None, text=unit_kwh))
			self.service.add_item(DoubleItem(prefix + '/Energy/Reverse', None, text=unit_kwh))

		await self.service.register()
		return True

	def destroy(self):
		if self.service is not None:
			self.service.__del__()
		self.service = None
		self.settings = None
		self.destroyed = True
	
	async def update(self, data):
		# NotifyStatus has power, current, voltage and energy values
		if self.service and data.get('method') == 'NotifyStatus':
			try:
				d = data['params']['em:0']
			except KeyError:
				pass
			else:
				with self.service as s:
					s['/Ac/L1/Voltage'] = d["a_voltage"]
					s['/Ac/L2/Voltage'] = d["b_voltage"]
					s['/Ac/L3/Voltage'] = d["c_voltage"]
					s['/Ac/L1/Current'] = d["a_current"]
					s['/Ac/L2/Current'] = d["b_current"]
					s['/Ac/L3/Current'] = d["c_current"]
					s['/Ac/L1/Power'] = d["a_act_power"]
					s['/Ac/L2/Power'] = d["b_act_power"]
					s['/Ac/L3/Power'] = d["c_act_power"]

					s['/Ac/Power'] = d["a_act_power"] + d["b_act_power"] + d["c_act_power"]

			try:
				d = data['params']['emdata:0']
			except KeyError:
				pass
			else:
				with self.service as s:
					s["/Ac/Energy/Forward"] = round(d["total_act"]/1000, 1)
					s["/Ac/Energy/Reverse"] = round(d["total_act_ret"]/1000, 1)
					s["/Ac/L1/Energy/Forward"] = round(d["a_total_act_energy"]/1000, 1)
					s["/Ac/L1/Energy/Reverse"] = round(d["a_total_act_ret_energy"]/1000, 1)
					s["/Ac/L2/Energy/Forward"] = round(d["b_total_act_energy"]/1000, 1)
					s["/Ac/L2/Energy/Reverse"] = round(d["b_total_act_ret_energy"]/1000, 1)
					s["/Ac/L3/Energy/Forward"] = round(d["c_total_act_energy"]/1000, 1)
					s["/Ac/L3/Energy/Reverse"] = round(d["c_total_act_ret_energy"]/1000, 1)

	def role_instance(self, value):
		val = value.split(':')
		return val[0], int(val[1])

	def settings_changed(self, service, values):
		settings = self.get_settings()
		if not settings:
			return

		# Check for custom name changes
		if self.settings_paths["customname"] in values:
			new_name = values[self.settings_paths["customname"]]
			self.update_custom_name(new_name)

		# Check for position changes in pvinverter role
		if self.settings_paths["position"] in values:
			new_position = values[self.settings_paths["position"]]
			self.update_position(new_position)

		# Restart for role/instance changes that require restart
		if self.settings_paths["instance"] in values:
			self.destroy()

	def update_custom_name(self, name):
		"""Update the custom name in the service"""
		if name == self.custom_name:
			return

		self.custom_name = name

		if self.service:
			with self.service as s:
				s['/CustomName'] = name

	def update_position(self, position):
		"""Update the position in the service"""
		if position == self.position:
			return

		self.position = position

		settings = self.get_settings()
		if not settings:
			return

		if self.service:
			role, _ = self.role_instance(settings.get_value(self.settings_paths["instance"]))
			if role == 'pvinverter':
				with self.service as s:
					s['/Position'] = position

	async def custom_name_changed(self, item, val):
		"""Handle custom name changes from the UI"""
		settings = self.get_settings()
		if settings is None:
			return False

		try:
			# Apply the change and update locally
			await settings.set_value(self.settings_paths["customname"], val)
			self.update_custom_name(val)
			return True
		except Exception as e:
			logger.error(f"Failed to update custom name: {e}")
			return False

	async def role_changed(self, item, val):
		"""Handle role changes from the UI"""
		if val not in ['grid', 'pvinverter', 'genset', 'acload']:
			return False

		settings = self.get_settings()
		if settings is None:
			return False

		try:
			# For role changes we need to completely restart the service
			_, instance = self.role_instance(settings.get_value(self.settings_paths["instance"]))
			await settings.set_value(self.settings_paths["instance"], f"{val}:{instance}")
			logger.info(f"Role changed to {val}, restarting service")
			self.destroy()  # restart is necessary for role changes
			return True
		except Exception as e:
			logger.error(f"Failed to change role: {e}")
			return False

	async def position_changed(self, item, val):
		"""Handle position changes from the UI"""
		if not 0 <= val <= 2:
			return False

		settings = self.get_settings()
		if settings is None:
			return False

		try:
			# Apply to settings and update locally
			await settings.set_value(self.settings_paths["position"], val)
			self.update_position(val)
			return True
		except Exception as e:
			logger.error(f"Failed to update position: {e}")
			return False
