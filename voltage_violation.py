# -------------------------------------------------------------------------------
# Copyright (c) 2017, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# -------------------------------------------------------------------------------
"""
Created on Jan 19, 2018

@author: Poorva Sharma
"""

import argparse
import json
import logging
import math
import sys
import time

from gridappsd import GridAPPSD, DifferenceBuilder, utils, topics
from gridappsd.topics import simulation_output_topic, simulation_log_topic, simulation_output_topic

class SimulationSubscriber(object):
    """ A simple class that handles publishing forward and reverse differences

    The object should be used as a callback from a GridAPPSD object so that the
    on_message function will get called each time a message from the simulator.  During
    the execution of on_meessage the `CapacitorToggler` object will publish a
    message to the simulation_input_topic with the forward and reverse difference specified.
    """

    def __init__(self, simulation_id, gridappsd_obj, nominal_voltage_map, message_interval):
        """ Create a ``SimulationSubscriber`` object

        This object is used as a subscription callback from a ``GridAPPSD``
        object.  This class receives simulation output and publishes 
	voltage violation with the frequency as defined by message_period.

        Parameters
        ----------
        simulation_id: str
            The simulation_id to use for publishing to a topic.
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.
        capacitor_list: list(str)
            A list of capacitors mrids to turn on/off
        """
        self._gapps = gridappsd_obj
        self.nominal_voltage_map = nominal_voltage_map
        self.message_interval = message_interval
        self._message_count = 0
        self._publish_to_topic = topics.service_output_topic('voltage_violation',simulation_id)

    def on_message(self, headers, message):
        """ Handle incoming messages on the simulation_output_topic for the simulation_id

        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """

        self._message_count += 1
        if self._message_count % self.message_interval == 0:
            #message = json.loads(message)
            voltage_violation = {}
            for meas_id in message['message']['measurements']:
                if meas_id in self.nominal_voltage_map:
                    mag = message['message']['measurements'][meas_id]['magnitude']
                    nv = int(self.nominal_voltage_map[meas_id])
                    puv = mag/(nv/math.sqrt(3))
                    if puv<0.95 or puv>1.05:
                       voltage_violation[meas_id] = round(puv,2)
            #print(voltage_violation)
            self._gapps.send(self._publish_to_topic, json.dumps(voltage_violation))


def get_nominal_voltage(gridappsd_obj, mrid):
    query = """
	PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	PREFIX c: <http://iec.ch/TC57/CIM100#>
	SELECT ?type ?name ?eqtype ?eqid ?id ?nv WHERE {
 	VALUES ?fdrid {"%s"}
	VALUES ?type {"PNV"}
	?eq c:Equipment.EquipmentContainer ?fdr.
	?fdr c:IdentifiedObject.mRID ?fdrid.
	{ ?s r:type c:Discrete. bind ("Discrete" as ?class)}
	UNION
	{ ?s r:type c:Analog. bind ("Analog" as ?class)}
	?s c:IdentifiedObject.name ?name .
	?s c:IdentifiedObject.mRID ?id .
	?s c:Measurement.PowerSystemResource ?eq .
	?s c:Measurement.Terminal ?trm .
	?s c:Measurement.measurementType ?type .
	?trm c:IdentifiedObject.mRID ?trmid.
	?eq c:IdentifiedObject.mRID ?eqid.
	?eq c:IdentifiedObject.name ?eqname.
	?eq r:type ?typeraw.
	bind(strafter(str(?typeraw),"#") as ?eqtype)
 	?te c:TransformerEnd.Terminal ?trm.
	?te c:TransformerEnd.BaseVoltage ?bv.
	?bv c:BaseVoltage.nominalVoltage ?nv.
	} ORDER BY ?class ?type ?name""" % mrid
    response = gridappsd_obj.query_data(query)
    nominal_voltage_map = {}
    for result in response['data']['results']['bindings']:
        nominal_voltage_map[result['id']['value']] = result['nv']['value']
    query = """
	PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	PREFIX c: <http://iec.ch/TC57/CIM100#>
	SELECT ?type ?name ?eqtype ?eqid ?id ?nv WHERE {
	 VALUES ?fdrid {"%s"}
	 VALUES ?type {"PNV"}
	 ?eq c:Equipment.EquipmentContainer ?fdr.
	 ?fdr c:IdentifiedObject.mRID ?fdrid.
	{ ?s r:type c:Discrete. bind ("Discrete" as ?class)}
	  UNION
	{ ?s r:type c:Analog. bind ("Analog" as ?class)}
	 ?s c:IdentifiedObject.name ?name .
	 ?s c:IdentifiedObject.mRID ?id .
	 ?s c:Measurement.PowerSystemResource ?eq .
	 ?s c:Measurement.Terminal ?trm .
	 ?s c:Measurement.measurementType ?type .
	 ?trm c:IdentifiedObject.mRID ?trmid.
	 ?eq c:IdentifiedObject.mRID ?eqid.
	 ?eq c:IdentifiedObject.name ?eqname.
	 ?eq r:type ?typeraw.
	  bind(strafter(str(?typeraw),"#") as ?eqtype)
	 ?trm c:Terminal.ConductingEquipment ?ce.
	 ?ce c:ConductingEquipment.BaseVoltage ?bv. 
	  ?bv c:BaseVoltage.nominalVoltage ?nv.
	} ORDER BY ?class ?type ?name""" % mrid
    response = gridappsd_obj.query_data(query)
    for result in response['data']['results']['bindings']:
        nominal_voltage_map[result['id']['value']] = result['nv']['value']
    return nominal_voltage_map


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    # These are now set through the docker container interface via env variables or defaulted to
    # proper values.
    #
    # parser.add_argument("-u", "--user", default="system",
    #                     help="The username to authenticate with the message bus.")
    # parser.add_argument("-p", "--password", default="manager",
    #                     help="The password to authenticate with the message bus.")
    # parser.add_argument("-a", "--address", default="127.0.0.1",
    #                     help="tcp address of the mesage bus.")
    # parser.add_argument("--port", default=61613, type=int,
    #                     help="the stomp port on the message bus.")
    #
    opts = parser.parse_args()
    listening_to_topic = simulation_output_topic(opts.simulation_id)
    #TODO: read this from simulation request as below 
    # message_interval = sim_request["service_config"]["voltage-violation"]["message_interval"]
    message_interval = 5
    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request["power_system_config"]["Line_name"]
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
    nominal_voltage_dict = get_nominal_voltage(gapps, model_mrid)
    subscriber = SimulationSubscriber(opts.simulation_id, gapps, nominal_voltage_dict, message_interval)
    gapps.subscribe(listening_to_topic, subscriber)
    while True:
        time.sleep(0.1)


if __name__ == "__main__":
    _main()
