/**
 * 'modbus': modbus wrapper to communicate with PLC throw RTU/TCP
 */

'use strict';
var ModbusRTU;
const datatypes = require('./datatypes');
const utils = require('../../utils');
const deviceUtils = require('../device-utils');
const net = require("net");
const TOKEN_LIMIT = 100;
const Mutex = require("async-mutex").Mutex;

function MODBUSclient(_data, _logger, _events, _runtime) {
    var memory = {};                        // Loaded Signal grouped by memory { memory index, start, size, ... }
    var data = JSON.parse(JSON.stringify(_data));                   // Current Device data { id, name, tags, enabled, ... }
    var logger = _logger;
    var client = new ModbusRTU();       // Client Modbus (Master)
    var working = false;                // Working flag to manage overloading polling and connection
    var events = _events;               // Events to commit change to runtime
    var lastStatus = '';                // Last Device status
    var varsValue = [];                 // Signale to send to frontend { id, type, value }
    var memItemsMap = {};               // Mapped Signale name with MemoryItem to find for set value
    var mixItemsMap = {};               // Map the fragmented Signale { key = start address, value = MemoryItems }
    var overloading = 0;                // Overloading counter to mange the break connection
    var lastTimestampValue;             // Last Timestamp of asked values
    var type;
    var runtime = _runtime;             // Access runtime config such as scripts
    var brokerServer = null;             // TCP listener when broker_mode is enabled
    var brokerSocket = null;             // active TCP client socket (USR converter)
    var brokerRxBuffer = Buffer.alloc(0);
    var brokerRequest = null;            // {resolve,reject,timer,expectedMin}

    /**
     * initialize the modubus type
     */
    this.init = function (_type) {
        type = _type;
    }

    /**
     * Connect to PLC
     * Emit connection status to clients, clear all Tags values
     */
    this.connect = function () {
        return new Promise(function (resolve, reject) {
            if (data.property && ((type === ModbusTypes.TCP && (data.property.broker_mode || data.property.address)) ||
                (type === ModbusTypes.RTU && data.property.baudrate && data.property.databits && data.property.stopbits && data.property.parity))) {
                try {
                    if (!client.isOpen && _checkWorking(true)) {
                        logger.info(`'${data.name}' try to connect ${data.property.address}`, true);
                        _connect(function (err) {
                            if (err) {
                                logger.error(`'${data.name}' connect failed! ${err}`);
                                _emitStatus('connect-error');
                                _clearVarsValue();
                                reject();
                            } else {
                                if (data.property.slaveid) {
                                    // set the client's unit id
                                    client.setID(parseInt(data.property.slaveid));
                                }
                                // set a timout for requests default is null (no timeout)
                                client.setTimeout(parseInt(data.property.modbus_timeout_ms) || 2000);
                                logger.info(`'${data.name}' connected!`, true);
                                _emitStatus('connect-ok');
                                resolve();
                            }
                            _checkWorking(false);
                        });
                    } else {
                        reject();
                        _emitStatus('connect-error');
                    }
                } catch (err) {
                    logger.error(`'${data.name}' try to connect error! ${err}`);
                    _checkWorking(false);
                    _emitStatus('connect-error');
                    _clearVarsValue();
                    reject();
                }
            } else {
                logger.error(`'${data.name}' missing connection data!`);
                _emitStatus('connect-failed');
                _clearVarsValue();
                reject();
            }
        });
    }

    /**
     * Disconnect the PLC
     * Emit connection status to clients, clear all Tags values
     */
    this.disconnect = function () {
        return new Promise(function (resolve, reject) {
            _checkWorking(false);
            _closeBrokerSocket();
            _closeBrokerServer();
            if (!client.isOpen || (type === ModbusTypes.TCP && data.property && data.property.broker_mode)) {
                _emitStatus('connect-off');
                _clearVarsValue();
                resolve(true);
            } else {
                client.close(function (result) {
                    if (result) {
                        logger.error(`'${data.name}' try to disconnect failed!`);
                    } else {
                        logger.info(`'${data.name}' disconnected!`, true);
                    }
                    _emitStatus('connect-off');
                    _clearVarsValue();
                    resolve(result);
                });
            }
        });
    }

    /**
     * Read values in polling mode
     * Update the tags values list, save in DAQ if value changed or in interval and emit values to clients
     */
    this.polling = async function () {
        let socketRelease;
        try {
            // Connexion/Resource reutilization logic (Socket/Serial) for TCP e RTU
            if (data.property.socketReuse) {
                let resourceKey;
                if (type === ModbusTypes.TCP) {
                    resourceKey = data.property.address;
                } else if (type === ModbusTypes.RTU) {
                    // Para RTU, usa o endereço da porta serial como identificador único do recurso
                    resourceKey = data.property.address;
                }

                if (resourceKey && runtime.socketMutex.has(resourceKey)) {
                    // Adquire o mutex para garantir acesso exclusivo ao recurso (socket TCP ou porta Serial RTU)
                    socketRelease = await runtime.socketMutex.get(resourceKey).acquire();
                }
            }

            await this._polling()
        } catch (err) {
            logger.error(`'${data.name}' polling! ${err}`);
        } finally {
            if (!utils.isNullOrUndefined(socketRelease)) {
                socketRelease()
            }
        }
    }
    this._polling = async function () {
        if (_checkWorking(true)) {
            var readVarsfnc = [];
            if (!data.property.options) {
                for (var memaddr in memory) {
                    var tokenizedAddress = parseAddress(memaddr);
                    try {
                        readVarsfnc.push(await _readMemory(parseInt(tokenizedAddress.address), memory[memaddr].Start, memory[memaddr].MaxSize, Object.values(memory[memaddr].Items)));
                        readVarsfnc.push(await delay(data.property.delay || 10));
                    } catch (err) {
                        logger.error(`'${data.name}' _readMemory error! ${err}`);
                    }
                }
            } else {
                for (var memaddr in mixItemsMap) {
                    try {
                        readVarsfnc.push(await _readMemory(getMemoryAddress(parseInt(memaddr), false), mixItemsMap[memaddr].Start, mixItemsMap[memaddr].MaxSize, Object.values(mixItemsMap[memaddr].Items)));
                        readVarsfnc.push(await delay(data.property.delay || 10));
                    } catch (err) {
                        logger.error(`'${data.name}' _readMemory error! ${err}`);
                    }
                }
            }
            // _checkWorking(false);
            try {
                const result = await Promise.all(readVarsfnc);

                _checkWorking(false);
                if (result.length) {
                    let varsValueChanged = await _updateVarsValue(result);
                    lastTimestampValue = new Date().getTime();
                    _emitValues(varsValue);
                    if (this.addDaq && !utils.isEmptyObject(varsValueChanged)) {
                        this.addDaq(varsValueChanged, data.name, data.id);
                    }
                } else {
                    // console.error('then error');
                }
                if (lastStatus !== 'connect-ok') {
                    _emitStatus('connect-ok');
                }
            } catch (reason) {
                if (reason) {
                    if (reason.stack) {
                        logger.error(`'${data.name}' _readVars error! ${reason.stack}`);
                    } else if (reason.message) {
                        logger.error(`'${data.name}' _readVars error! ${reason.message}`);
                    }
                } else {
                    logger.error(`'${data.name}' _readVars error! ${reason}`);
                }
                _checkWorking(false);
            };
        } else {
            _emitStatus('connect-busy');
        }
    }

    /**
     * Load Tags attribute to read with polling
     */
    this.load = function (_data) {
        data = JSON.parse(JSON.stringify(_data));
        memory = {};
        varsValue = [];
        // memItemsMap = {};
        mixItemsMap = {};   // Map the fragmented tag { key = start address, value = MemoryItems }
        var stepsMap = {};  // Map the tag start address and size { key = start address, value = signal size and offset }
        var count = 0;
        for (var id in data.tags) {
            try {
                var offset = parseInt(data.tags[id].address) - 1;   // because settings address from 1 to 65536 but communication start from 0
                var token = Math.trunc(offset / TOKEN_LIMIT);
                var memaddr = formatAddress(data.tags[id].memaddress, token);
                if (!memory[memaddr]) {
                    memory[memaddr] = new MemoryItems();
                }
                if (!memory[memaddr].Items[offset]) {
                    memory[memaddr].Items[offset] = new MemoryItem(data.tags[id].type, offset);
                }
                memory[memaddr].Items[offset].Tags.push(data.tags[id]); // because you can have multiple tags at the same DB address

                if (offset < memory[memaddr].Start) {
                    if (memory[memaddr].Start != 65536) {
                        memory[memaddr].MaxSize += memory[memaddr].Start - offset;
                        memory[memaddr].Start = offset;
                    } else {
                        memory[memaddr].MaxSize = datatypes[data.tags[id].type].WordLen;
                        memory[memaddr].Start = offset;
                    }
                } else {
                    var len = offset + datatypes[data.tags[id].type].WordLen - memory[memaddr].Start;
                    if (memory[memaddr].MaxSize < len) {
                        memory[memaddr].MaxSize = len;
                    }
                }
                memItemsMap[id] = memory[memaddr].Items[offset];
                memItemsMap[id].format = data.tags[id].format;
                stepsMap[parseInt(data.tags[id].memaddress) + offset] = { size: datatypes[data.tags[id].type].WordLen, offset: offset };
            } catch (err) {
                logger.error(`'${data.name}' load error! ${err}`);
            }
        }
        // for fragmented
        let lastStart = -1;             // last start address
        let lastMemAdr = -1;
        let nextAdr = -1;
        Object.keys(stepsMap).sort((a, b) => { return a - b; }).forEach(function (key) {
            try {
                var adr = parseInt(key);        // tag address
                let lastAdrSize = adr + stepsMap[key].size;
                let offset = stepsMap[key].offset;
                if (nextAdr < adr) {
                    // to fragment then new range
                    lastStart = adr;
                    let mits = new MemoryItems();
                    mits.Start = lastStart - getMemoryAddress(lastStart, false);
                    mits.MaxSize = lastAdrSize - lastStart;
                    var token = Math.trunc(offset / TOKEN_LIMIT);
                    lastMemAdr = getMemoryAddress(lastStart, true, token);
                    mits.Items = getMemoryItems(memory[lastMemAdr].Items, mits.Start, mits.MaxSize);
                    mixItemsMap[lastStart] = mits;
                } else if (mixItemsMap[lastStart]) {
                    // to attach of exist range
                    mixItemsMap[lastStart].MaxSize = lastAdrSize - lastStart;
                    mixItemsMap[lastStart].Items = getMemoryItems(memory[lastMemAdr].Items, mixItemsMap[lastStart].Start, mixItemsMap[lastStart].MaxSize);
                }
                nextAdr = 1 + adr + stepsMap[key].size;
            } catch (err) {
                logger.error(`'${data.name}' load error! ${err}`);
            }
        });
        logger.info(`'${data.name}' data loaded (${count})`, true);
    }

    /**
     * Return Tags values array { id: <name>, value: <value>, type: <type> }
     */
    this.getValues = function () {
        return varsValue;
    }

    /**
     * Return Tag value { id: <name>, value: <value>, ts: <lastTimestampValue> }
     */
    this.getValue = function (id) {
        if (varsValue[id]) {
            return { id: id, value: varsValue[id].value, ts: lastTimestampValue };
        }
        return null;
    }

    /**
     * Return connection status 'connect-off', 'connect-ok', 'connect-error'
     */
    this.getStatus = function () {
        return lastStatus;
    }

    /**
     * Return Tag property
     */
    this.getTagProperty = function (id) {
        if (memItemsMap[id]) {
            return { id: id, name: id, type: memItemsMap[id].type, format: memItemsMap[id].format };
        } else {
            return null;
        }
    }

    /**
     * Set the Tag value
     * Read the current Tag object, write the value in object and send to SPS
     */
    this.setValue = async function (sigid, value) {
        if (data.tags[sigid]) {
            var memaddr = data.tags[sigid].memaddress;
            var offset = parseInt(data.tags[sigid].address) - 1;   // because settings address from 1 to 65536 but communication start from 0
            value = await deviceUtils.tagRawCalculator(value, data.tags[sigid]);

            const divVal = convertValue(value, data.tags[sigid].divisor, true);
            var val;
            if (data.tags[sigid].scaleWriteFunction) {
                let parameters = [
                    { name: 'value', type: 'value', value: divVal }
                ];
                if (data.tags[sigid].scaleWriteParams) {
                    const extraParamsWithValues = JSON.parse(data.tags[sigid].scaleWriteParams);
                    parameters = [...parameters, ...extraParamsWithValues];

                }
                const script = {
                    id: data.tags[sigid].scaleWriteFunction,
                    name: null,
                    parameters
                };
                try {
                    const bufVal = await runtime.scriptsMgr.runScript(script, false);
                    if (Array.isArray(bufVal)) {
                        if ((bufVal.length % 2) !== 0) {
                            logger.error(`'${data.tags[sigid].name}' setValue script error, returned buffer invalid must be mod 2`);
                            return false;
                        }
                        val = [];
                        for (let i = 0; i < bufVal.length;) {
                            val.push(bufVal.readUInt16BE(i));
                            i = i + 2;
                        }
                    } else {
                        val = bufVal;
                    }
                } catch (error) {
                    logger.error(`'${data.tags[sigid].name}' setValue script error! ${error.toString()}`);
                    return false;
                }

            } else {
                val = datatypes[data.tags[sigid].type].formatter(divVal);
            }

            // Wait logic for RTU removed, o Mutex will control concurrency.
            // if (type === ModbusTypes.RTU) {
            //     const start = Date.now();
            //     let now = start;
            //     while ((now - start) < 3000 && working) {  // wait max 3 seconds
            //         now = Date.now();
            //         await delay(20);
            //     }
            // }

            let socketRelease;
            try {
                // Connexion/Resource reutilization logic (Socket/Serial) for TCP and RTU
                if (data.property.socketReuse) {
                    let resourceKey;
                    if (type === ModbusTypes.TCP || type === ModbusTypes.RTU) {
                        // For TCP: socket addres. For RTU: serial port address
                        resourceKey = data.property.address;
                    }

                    if (resourceKey && runtime.socketMutex.has(resourceKey)) {
                        socketRelease = await runtime.socketMutex.get(resourceKey).acquire();
                    }
                }

                _checkWorking(true);

                await _writeMemory(parseInt(memaddr), offset, val).then(result => {
                    logger.info(`'${data.name}' setValue(${sigid}, ${value})`, true, true);
                }, reason => {
                    if (reason && reason.stack) {
                        logger.error(`'${data.name}' _writeMemory error! ${reason.stack}`);
                    } else {
                        logger.error(`'${data.name}' _writeMemory error! ${reason}`);
                    }
                });
            } catch (err) {
                logger.error(`'${data.name}' setValue error! ${err}`);
            } finally {
                _checkWorking(false);
                if (!utils.isNullOrUndefined(socketRelease)) {
                    socketRelease();
                }
            }
            return true;
        } else {
            logger.error(`'${data.name}' setValue(${sigid}, ${value}) Tag not found`, true, true);
        }
        return false;
    }

    /**
     * Return if PLC is connected
     * Don't work if PLC will disconnect
     */
    this.isConnected = function () {
        if (type === ModbusTypes.TCP && data.property && data.property.broker_mode) {
            return !!brokerSocket && !brokerSocket.destroyed;
        }
        return client.isOpen;
    }

    /**
     * Bind the DAQ store function
     */
    this.bindAddDaq = function (fnc) {
        this.addDaq = fnc;                         // Add the DAQ value to db history
    }

    this.addDaq = null;

    /**
     * Return the timestamp of last read tag operation on polling
     * @returns
     */
    this.lastReadTimestamp = () => {
        return lastTimestampValue;
    }

    /**
     * Return the Daq settings of Tag
     * @returns
     */
    this.getTagDaqSettings = (tagId) => {
        return data.tags[tagId] ? data.tags[tagId].daq : null;
    }

    /**
     * Set Daq settings of Tag
     * @returns
     */
    this.setTagDaqSettings = (tagId, settings) => {
        if (data.tags[tagId]) {
            utils.mergeObjectsValues(data.tags[tagId].daq, settings);
        }
    }

    /**
     * Connect with RTU or TCP
     */
    var _connect = function (callback) {
        try {
            if (type === ModbusTypes.RTU) {
                const rtuOptions = {
                    baudRate: parseInt(data.property.baudrate),
                    dataBits: parseInt(data.property.databits),
                    stopBits: parseFloat(data.property.stopbits),
                    parity: data.property.parity.toLowerCase()
                }

                // >>> ADDED: Initialize the mutex for  Modbus RTU (porta serial) <<<
                if (data.property.socketReuse === ModbusReuseModeType.ReuseSerial && !runtime.socketMutex.has(data.property.address)) {
                    // port address (data.property.address) is the key for the resource
                    runtime.socketMutex.set(data.property.address, new Mutex());
                }
                // >>> END OF CHANGE <<<

                if (data.property.connectionOption === ModbusOptionType.RTUBufferedPort) {
                    client.connectRTUBuffered(data.property.address, rtuOptions, callback);
                } else if (data.property.connectionOption === ModbusOptionType.AsciiPort) {
                    client.connectAsciiSerial(data.property.address, rtuOptions, callback);
                } else {
                    client.connectRTU(data.property.address, rtuOptions, callback);
                }
            } else if (type === ModbusTypes.TCP) {
                if (data.property.broker_mode) {
                    _startBrokerServer(callback);
                    return;
                }
                var port = 502;
                var addr = data.property.address;
                if (data.property.address.indexOf(':') !== -1) {
                    addr = data.property.address.substring(0, data.property.address.indexOf(':'));
                    var temp = data.property.address.substring(data.property.address.indexOf(':') + 1);
                    port = parseInt(temp);
                }
                //reuse socket
                if (data.property.socketReuse) {
                    var socket;
                    if (runtime.socketPool.has(data.property.address)) {
                        socket = runtime.socketPool.get(data.property.address);
                    } else {
                        socket = new net.Socket();
                        runtime.socketPool.set(data.property.address, socket);
                        //init read mutex
                        if (data.property.socketReuse === ModbusReuseModeType.ReuseSerial) {
                            runtime.socketMutex.set(data.property.address, new Mutex())
                        }
                    }
                    var openFlag = socket.readyState === "opening" || socket.readyState === "open";
                    if (!openFlag) {
                        socket.connect({
                            // Default options
                            ...{
                                host: addr,
                                port: port
                            },
                        });
                    }
                }
                if (data.property.connectionOption === ModbusOptionType.UdpPort) {
                    client.connectUDP(addr, { port: port }, callback);
                } else if (data.property.connectionOption === ModbusOptionType.TcpRTUBufferedPort) {
                    if (data.property.socketReuse) {
                        client.linkTcpRTUBuffered(runtime.socketPool.get(data.property.address), callback);
                    } else {
                        client.connectTcpRTUBuffered(addr, { port: port }, callback);
                    }
                } else if (data.property.connectionOption === ModbusOptionType.TelnetPort) {
                    if (data.property.socketReuse) {
                        client.linkTelnet(runtime.socketPool.get(data.property.address), callback);
                    } else {
                        client.connectTelnet(addr, { port: port }, callback);
                    }
                } else {
                    //reuse socket
                    if (data.property.socketReuse) {
                        client.linkTCP(runtime.socketPool.get(data.property.address), callback);
                    } else {
                        client.connectTCP(addr, { port: port }, callback);
                    }
                }
            }
        } catch (err) {
            callback(err);
        }
    }

    /**
     * Read a Memory from modbus and parse the result
     * @param {int} memoryAddress - The memory address to read
     * @param {int} start - Position of the first variable
     * @param {int} size - Length of the variables to read (the last address)
     * @param {array} vars - Array of Var objects
     * @returns {Promise} - Resolves to the vars array with populate *value* property
     */
    var _readMemory = function (memoryAddress, start, size, vars) {
        return new Promise((resolve, reject) => {
            if (vars.length === 0) return resolve([]);
            if (type === ModbusTypes.TCP && data.property && data.property.broker_mode) {
                return _readMemoryBroker(memoryAddress, start, size, vars).then(resolve).catch(reject);
            }
            // define read function
            if (memoryAddress === ModbusMemoryAddress.CoilStatus) {                      // Coil Status (Read/Write 000001-065536)
                client.readCoils(start, size).then(res => {
                    if (res.data) {
                        vars.map(v => {
                            let bitoffset = Math.trunc((v.offset - start) / 8);
                            let bit = (v.offset - start) % 8;
                            let value = datatypes[v.type].parser(res.buffer, bitoffset, bit);
                            v.changed = value !== v.rawValue;
                            v.rawValue = value;
                        });
                    }
                    resolve(vars);
                }, reason => {
                    reject(reason);
                });
            } else if (memoryAddress === ModbusMemoryAddress.DigitalInputs) {          // Digital Inputs (Read 100001-165536)
                client.readDiscreteInputs(start, size).then(res => {
                    if (res.data) {
                        vars.map(v => {
                            let bitoffset = Math.trunc((v.offset - start) / 8);
                            let bit = (v.offset - start) % 8;
                            let value = datatypes[v.type].parser(res.buffer, bitoffset, bit);
                            v.changed = value !== v.rawValue;
                            v.rawValue = value;
                        });
                    }
                    resolve(vars);
                }, reason => {
                    reject(reason);
                });
            } else if (memoryAddress === ModbusMemoryAddress.InputRegisters) {          // Input Registers (Read  300001-365536)
                client.readInputRegisters(start, size).then(res => {
                    if (res.data) {
                        vars.map(v => {
                            try {
                                let byteoffset = (v.offset - start) * 2;
                                let buffer = Buffer.from(res.buffer.slice(byteoffset, byteoffset + datatypes[v.type].bytes))
                                let value = datatypes[v.type].parser(buffer);
                                v.changed = value !== v.rawValue;
                                v.rawValue = value;
                            } catch (err) {
                                console.error(err);
                            }
                        });
                    }
                    resolve(vars);
                }, reason => {
                    reject(reason);
                });
            } else if (memoryAddress === ModbusMemoryAddress.HoldingRegisters) {          // Holding Registers (Read/Write  400001-465535)
                client.readHoldingRegisters(start, size).then(res => {
                    if (res.data) {
                        vars.map(v => {
                            let byteoffset = (v.offset - start) * 2;
                            let buffer = Buffer.from(res.buffer.slice(byteoffset, byteoffset + datatypes[v.type].bytes))
                            let value = datatypes[v.type].parser(buffer);
                            v.changed = value !== v.rawValue;
                            v.rawValue = value;
                        });
                    }
                    resolve(vars);
                }, reason => {
                    console.error(reason);
                    reject(reason);
                });
            } else {
                reject();
            }
        });
    }

    /**
     * Write value to modbus
     * @param {*} memoryAddress
     * @param {*} start
     * @param {*} value
     */
    var _writeMemory = function (memoryAddress, start, value) {
        return new Promise((resolve, reject) => {
            if (type === ModbusTypes.TCP && data.property && data.property.broker_mode) {
                return _writeMemoryBroker(memoryAddress, start, value).then(resolve).catch(reject);
            }
            if (memoryAddress === ModbusMemoryAddress.CoilStatus) {                      // Coil Status (Read/Write 000001-065536)
                client.writeCoil(start, value).then(res => {
                    resolve();
                }, reason => {
                    console.error(reason);
                    reject(reason);
                });
            } else if (memoryAddress === ModbusMemoryAddress.DigitalInputs) {           // Digital Inputs (Read 100001-165536)
                reject();
            } else if (memoryAddress === ModbusMemoryAddress.InputRegisters) {          // Input Registers (Read  300001-365536)
                reject();
            } else if (memoryAddress === ModbusMemoryAddress.HoldingRegisters) {
                // Utiliser forceFC16 depuis la config du device
                if (value.length > 2 || data.property.forceFC16) {
                    client.writeRegisters(start, value).then(res => {
                        resolve();
                    }, reason => {
                        console.error(reason);
                        reject(reason);
                    });
                } else {
                    client.writeRegister(start, value).then(res => {
                        resolve();
                    }, reason => {
                        console.error(reason);
                        reject(reason);
                    });
                }
            } else {
                reject();
            }
        });
    }

    /**
     * Clear the Tags values by setting to null
     * Emit to clients
     */
    var _clearVarsValue = function () {
        for (var id in varsValue) {
            varsValue[id].value = null;
        }
        for (var id in memItemsMap) {
            memItemsMap[id].value = null;
        }
        _emitValues(varsValue);
    }

    /**
     * Update the Tags values read
     * @param {*} vars
     */
    var _updateVarsValue = async (vars) => {
        var someval = false;
        var tempTags = {};
        for (var vid in vars) {
            let items = vars[vid];
            for (var itemidx in items) {
                const changed = items[itemidx].changed;
                if (items[itemidx] instanceof MemoryItem) {
                    let type = items[itemidx].type;
                    let rawValue = items[itemidx].rawValue;
                    let tags = items[itemidx].Tags;
                    tags.forEach(tag => {
                        tempTags[tag.id] = {
                            id: tag.id,
                            rawValue: convertValue(rawValue, tag.divisor),
                            type: type,
                            daq: tag.daq,
                            changed: changed,
                            tagref: tag
                        };
                        someval = true;
                    });
                } else {
                    tempTags[items[itemidx].id] = {
                        id: items[itemidx].id,
                        rawValue: items[itemidx].rawValue,
                        type: items[itemidx].type,
                        daq: items[itemidx].daq,
                        changed: changed,
                        tagref: items[itemidx]
                    };
                    someval = true;
                }
            }
        }
        if (someval) {
            const timestamp = new Date().getTime();
            var result = {};
            for (var id in tempTags) {
                if (!utils.isNullOrUndefined(tempTags[id].rawValue)) {
                    tempTags[id].value = await deviceUtils.tagValueCompose(tempTags[id].rawValue, varsValue[id] ? varsValue[id].value : null, tempTags[id].tagref, runtime);
                    tempTags[id].timestamp = timestamp;
                    if (this.addDaq && deviceUtils.tagDaqToSave(tempTags[id], timestamp)) {
                        result[id] = tempTags[id];
                    }
                }
                varsValue[id] = tempTags[id];
                varsValue[id].changed = false;
            }
            return result;
        }
        return null;
    }

    /**
     * Emit the PLC Tags values array { id: <name>, value: <value>, type: <type> }
     * @param {*} values
     */
    var _emitValues = function (values) {
        events.emit('device-value:changed', { id: data.id, values: values });
    }

    /**
     * Emit the PLC connection status
     * @param {*} status
     */
    var _emitStatus = function (status) {
        lastStatus = status;
        events.emit('device-status:changed', { id: data.id, status: status });
    }

    /**
     * Used to manage the async connection and polling automation (that not overloading)
     * @param {*} check
     */
    var _checkWorking = function (check) {
        if (check && working) {
            overloading++;
            // !The driver don't give the break connection
            if (overloading >= 3) {
                if (type !== ModbusTypes.RTU) {
                    logger.warn(`'${data.name}' working (connection || polling) overload! ${overloading}`);
                }
                client.close();
            } else {
                return false;
            }
        }
        working = check;
        overloading = 0;
        return true;
    }


    var _startBrokerServer = function (callback) {
        const listenPort = parseInt(data.property.broker_port) || 502;
        try {
            _closeBrokerServer();
            brokerServer = net.createServer((socket) => {
                logger.info(`'${data.name}' broker client connected from ${socket.remoteAddress}:${socket.remotePort}`, true);
                _attachBrokerSocket(socket);
            });
            brokerServer.on('error', (err) => {
                logger.error(`'${data.name}' broker listener error! ${err}`);
            });
            brokerServer.listen(listenPort, '0.0.0.0', () => {
                logger.info(`'${data.name}' broker listener active on 0.0.0.0:${listenPort}`, true);
                _emitStatus('connect-off');
                callback();
            });
        } catch (err) {
            callback(err);
        }
    }

    var _attachBrokerSocket = function (socket) {
        if (brokerSocket && !brokerSocket.destroyed) {
            brokerSocket.destroy();
        }
        _resetBrokerState();
        brokerSocket = socket;
        brokerSocket.setNoDelay(true);
        brokerSocket.setKeepAlive(true);
        _emitStatus('connect-ok');

        brokerSocket.on('data', (chunk) => {
            _onBrokerData(chunk);
        });
        brokerSocket.on('error', (err) => {
            logger.error(`'${data.name}' broker socket error! ${err}`);
            _closeBrokerSocket();
            _emitStatus('connect-off');
        });
        brokerSocket.on('close', () => {
            _closeBrokerSocket();
            _emitStatus('connect-off');
        });
    }

    var _closeBrokerSocket = function () {
        if (brokerSocket) {
            try { brokerSocket.removeAllListeners(); } catch { }
            try { brokerSocket.destroy(); } catch { }
        }
        brokerSocket = null;
        _resetBrokerState();
    }

    var _closeBrokerServer = function () {
        _closeBrokerSocket();
        if (brokerServer) {
            try { brokerServer.close(); } catch { }
            brokerServer = null;
        }
    }

    var _resetBrokerState = function () {
        brokerRxBuffer = Buffer.alloc(0);
        if (brokerRequest) {
            if (brokerRequest.timer) {
                clearTimeout(brokerRequest.timer);
            }
            if (brokerRequest.reject) {
                brokerRequest.reject(new Error('broker socket closed'));
            }
        }
        brokerRequest = null;
    }

    var _onBrokerData = function (chunk) {
        if (!chunk || !chunk.length) {
            return;
        }
        brokerRxBuffer = Buffer.concat([brokerRxBuffer, chunk]);
        if (!brokerRequest) {
            return;
        }
        const parsed = _extractBrokerFrame();
        if (parsed && brokerRequest) {
            const request = brokerRequest;
            brokerRequest = null;
            clearTimeout(request.timer);
            request.resolve(parsed);
        }
    }

    var _extractBrokerFrame = function () {
        if (!brokerRequest || brokerRxBuffer.length < 5) {
            return null;
        }
        for (let i = 0; i <= brokerRxBuffer.length - 5; i++) {
            const unitId = brokerRxBuffer[i];
            const fn = brokerRxBuffer[i + 1];
            let expectedLength = 0;
            if (fn === 0x01 || fn === 0x02 || fn === 0x03 || fn === 0x04) {
                const byteCount = brokerRxBuffer[i + 2];
                expectedLength = 3 + byteCount + 2;
            } else if (fn === 0x05 || fn === 0x06 || fn === 0x10) {
                expectedLength = 8;
            } else if (fn & 0x80) {
                expectedLength = 5;
            } else {
                continue;
            }
            if (i + expectedLength > brokerRxBuffer.length) {
                return null;
            }
            const frame = brokerRxBuffer.slice(i, i + expectedLength);
            if (_crc16(frame.slice(0, -2)) !== frame.readUInt16LE(frame.length - 2)) {
                continue;
            }
            if (brokerRequest.unitId !== undefined && brokerRequest.unitId !== unitId) {
                brokerRxBuffer = brokerRxBuffer.slice(i + expectedLength);
                continue;
            }
            brokerRxBuffer = brokerRxBuffer.slice(i + expectedLength);
            return frame;
        }
        if (brokerRxBuffer.length > 2048) {
            brokerRxBuffer = Buffer.alloc(0);
        }
        return null;
    }

    var _sendRtuRequest = function (pdu, unitId) {
        return new Promise((resolve, reject) => {
            if (!brokerSocket || brokerSocket.destroyed) {
                return reject(new Error('broker socket not connected'));
            }
            if (brokerRequest) {
                return reject(new Error('broker busy'));
            }
            const uid = unitId || parseInt(data.property.slaveid) || 1;
            const payload = Buffer.concat([Buffer.from([uid]), pdu]);
            const crc = _crc16(payload);
            const frame = Buffer.concat([payload, Buffer.from([crc & 0xFF, (crc >> 8) & 0xFF])]);
            const timeout = parseInt(data.property.modbus_timeout_ms) || 2000;
            brokerRequest = {
                unitId: uid,
                timer: setTimeout(() => {
                    if (brokerRequest) {
                        brokerRequest = null;
                        reject(new Error(`modbus timeout ${timeout}ms`));
                    }
                }, timeout),
                resolve,
                reject
            };
            brokerSocket.write(frame, (err) => {
                if (err && brokerRequest) {
                    clearTimeout(brokerRequest.timer);
                    brokerRequest = null;
                    reject(err);
                }
            });
        });
    }

    var _readMemoryBroker = async function (memoryAddress, start, size, vars) {
        let fn = 0;
        if (memoryAddress === ModbusMemoryAddress.CoilStatus) fn = 0x01;
        else if (memoryAddress === ModbusMemoryAddress.DigitalInputs) fn = 0x02;
        else if (memoryAddress === ModbusMemoryAddress.InputRegisters) fn = 0x04;
        else if (memoryAddress === ModbusMemoryAddress.HoldingRegisters) fn = 0x03;
        else throw new Error('invalid memory address');

        const pdu = Buffer.from([fn, (start >> 8) & 0xFF, start & 0xFF, (size >> 8) & 0xFF, size & 0xFF]);
        const frame = await _sendRtuRequest(pdu);
        const responseFn = frame[1];
        if (responseFn & 0x80) {
            throw new Error(`modbus exception ${frame[2]}`);
        }
        const dataBuffer = frame.slice(3, frame.length - 2);
        vars.map(v => {
            if (memoryAddress === ModbusMemoryAddress.CoilStatus || memoryAddress === ModbusMemoryAddress.DigitalInputs) {
                let bitoffset = Math.trunc((v.offset - start) / 8);
                let bit = (v.offset - start) % 8;
                let value = datatypes[v.type].parser(dataBuffer, bitoffset, bit);
                v.changed = value !== v.rawValue;
                v.rawValue = value;
            } else {
                let byteoffset = (v.offset - start) * 2;
                let buffer = Buffer.from(dataBuffer.slice(byteoffset, byteoffset + datatypes[v.type].bytes));
                let value = datatypes[v.type].parser(buffer);
                v.changed = value !== v.rawValue;
                v.rawValue = value;
            }
        });
        return vars;
    }

    var _writeMemoryBroker = async function (memoryAddress, start, value) {
        let pdu;
        if (memoryAddress === ModbusMemoryAddress.CoilStatus) {
            const boolVal = Array.isArray(value) ? !!value[0] : !!value;
            pdu = Buffer.from([0x05, (start >> 8) & 0xFF, start & 0xFF, boolVal ? 0xFF : 0x00, 0x00]);
        } else if (memoryAddress === ModbusMemoryAddress.HoldingRegisters) {
            if (value.length > 2 || data.property.forceFC16) {
                const regs = Array.isArray(value) ? value : [value];
                const bytes = Buffer.alloc(regs.length * 2);
                regs.forEach((r, idx) => bytes.writeUInt16BE(parseInt(r), idx * 2));
                pdu = Buffer.concat([Buffer.from([0x10, (start >> 8) & 0xFF, start & 0xFF, (regs.length >> 8) & 0xFF, regs.length & 0xFF, bytes.length]), bytes]);
            } else {
                const val = Array.isArray(value) ? parseInt(value[0]) : parseInt(value);
                pdu = Buffer.from([0x06, (start >> 8) & 0xFF, start & 0xFF, (val >> 8) & 0xFF, val & 0xFF]);
            }
        } else {
            throw new Error('invalid write memory address for broker mode');
        }
        const frame = await _sendRtuRequest(pdu);
        const responseFn = frame[1];
        if (responseFn & 0x80) {
            throw new Error(`modbus exception ${frame[2]}`);
        }
    }

    var _crc16 = function (buffer) {
        let crc = 0xFFFF;
        for (let pos = 0; pos < buffer.length; pos++) {
            crc ^= buffer[pos];
            for (let i = 0; i < 8; i++) {
                if ((crc & 0x0001) !== 0) {
                    crc >>= 1;
                    crc ^= 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }

    const formatAddress = function (address, token) { return token + '-' + address; }
    const parseAddress = function (address) { return { token: address.split('-')[0], address: address.split('-')[1] }; }
    const getMemoryAddress = function (address, askey, token) {
        if (address < ModbusMemoryAddress.DigitalInputs) {
            if (askey) {
                return formatAddress('000000', token);
            }
            return ModbusMemoryAddress.CoilStatus;
        } else if (address < ModbusMemoryAddress.InputRegisters) {
            if (askey) {
                return formatAddress(ModbusMemoryAddress.DigitalInputs, token);
            }
            return ModbusMemoryAddress.DigitalInputs;
        } else if (address < ModbusMemoryAddress.HoldingRegisters) {
            if (askey) {
                return formatAddress(ModbusMemoryAddress.InputRegisters, token);
            }
            return ModbusMemoryAddress.InputRegisters;
        } else {
            if (askey) {
                return formatAddress(ModbusMemoryAddress.HoldingRegisters, token);
            }
            return ModbusMemoryAddress.HoldingRegisters;
        }
    }
    const convertValue = function (value, divisor, tosrc = false) {
        try {
            if (divisor && parseFloat(divisor)) {
                if (tosrc) {
                    return value * parseFloat(divisor);
                } else {
                    return value / parseFloat(divisor);
                }
            }
        } catch (err) {
            console.error(err);
        }
        return value;
    }

    /**
     * Return the Items that are wit address and size in the range start, size
     * @param {*} items
     * @param {*} start
     * @param {*} size
     * @returns
     */
    const getMemoryItems = function (items, start, size) {
        let result = {};
        for (var itemidx in items) {
            if (items[itemidx].offset >= start && items[itemidx].offset < start + size) {
                result[itemidx] = items[itemidx];
            }
        }
        return result;
    }
    const delay = ms => { return new Promise(resolve => setTimeout(resolve, ms)) };
}

const ModbusTypes = { RTU: 0, TCP: 1 };
const ModbusMemoryAddress = { CoilStatus: 0, DigitalInputs: 100000, InputRegisters: 300000, HoldingRegisters: 400000 };
const ModbusOptionType = {
    SerialPort: 'SerialPort',
    RTUBufferedPort: 'RTUBufferedPort',
    AsciiPort: 'AsciiPort',
    TcpPort: 'TcpPort',
    UdpPort: 'UdpPort',
    TcpRTUBufferedPort: 'TcpRTUBufferedPort',
    TelnetPort: 'TelnetPort'
}
const ModbusReuseModeType = {
    Reuse: 'Reuse',
    ReuseSerial: 'ReuseSerial',
}

module.exports = {
    init: function (settings) {
        // deviceCloseTimeout = settings.deviceCloseTimeout || 15000;
    },
    create: function (data, logger, events, manager, runtime) {
        try { ModbusRTU = require('modbus-serial'); } catch { }
        if (!ModbusRTU && manager) { try { ModbusRTU = manager.require('modbus-serial'); } catch { } }
        if (!ModbusRTU) return null;
        return new MODBUSclient(data, logger, events, runtime);
    },
    ModbusTypes: ModbusTypes
}

function MemoryItem(type, offset) {
    this.offset = offset;
    this.type = type;
    this.bit = -1;
    this.Tags = [];
}

function MemoryItems() {
    this.Start = 65536;
    this.MaxSize = 0;
    this.Items = {};
}
