// A RandomAccessStorage provider designed to persist state to individual files
// while handling many (10,000+) instances in memory without exhausing file
// handles.
//
// Implementation sketch:
//
// * PathWorker: responsible for an individual file. Opens or closes the file
//   and (when open) executes its assigned read/write ops according to
//   instructions from the RandomAccessFilesystem. Modeled as a state machine.
//
// * PathInterface: implements that RandomAccess interface for users (just read
//   & write currently). Passes these requests back to the central
//   RandomAccessFilesystem instance for dispatching, because dispatches need
//   to consider ops accross all files.
//
// * RandomAccessFilesystem: Manages a directory of files and associated file
//   handles. Schedules opening and closing of files so that we never go above
//   the max number of allowed open files.


const Path = require('path')
const FS = require('fs')
const Debug = require('debug')
const mkdirp = require('mkdirp')
const assert = require('assert')
const Deque = require('double-ended-queue')

const READWRITE = FS.constants.O_RDWR | FS.constants.O_CREAT

const log = Debug('pushpin:random-access-filesystem')

class PathWorker {
  constructor(fullPath) {
    this.fullPath = fullPath
    this.fullDirPath = Path.dirname(fullPath)
    this.fullDirMade = false
    this.control = 'stop'
    this.state = 'closed'
    this.fd = 0
    this.ops = []
    this.inFlightReads = 0
    this.inFlightWrites = 0
    this.startCb = null
    this.stopCb = null
    this.log = (...args) => { log(this.fullPath, ...args) }
  }

  add(op) {
    this.log('add')
    this.ops.push(op)
    this.tick()
  }

  start(startCb) {
    this.log('start')
    this.control = 'start'
    assert(!this.startCb)
    this.startCb = startCb
    this.tick()
  }

  stop(stopCb) {
    this.log('stop')
    this.control = 'stop'
    assert(!this.stopCb)
    this.stopCb = stopCb
    this.tick()
  }

  tick() {
    this.log('tick', this.control, this.state)
    switch (this.control) {
    case 'start':
      switch (this.state) {
      case 'opening':
        // Wait til we get to opened.
        break
      case 'opened':
        // Exec with open fd. Notify if we haven't yet.
        this.execIfOps()
        if (this.startCb) {
          this.startCb()
          this.startCb = null
        }
        break
      case 'draining':
        // Go back to execing as usual with existing open FD.
        this.state = 'opened'
        this.tick()
        break
      case 'closing':
        // Wait til we get to closed.
        break
      case 'closed':
        // Start opening.
        this.open()
        break
      default:
        throw new Error(`Unrecognized state: ${this.state}`)
      }
      break
    case 'stop':
      switch (this.state) {
      case 'opening':
        // Wait til we get to opened.
        break
      case 'opened':
        // Start draining, though one-time exec any outstanding ops.
        this.execIfOps()
        this.state = 'draining'
        this.tick()
        break
      case 'draining':
        this.closeIfDrained()
        break
      case 'closing':
        // Wait til we get to closed.
        break
      case 'closed':
        // Notify that we've closed. Otherwise wait patiently.
        if (this.stopCb) {
          this.stopCb()
          this.stopCb = null
        }
        break
      default:
        throw new Error(`Unrecognized state: ${this.state}`)
      }
      break
    default:
      throw new Error(`Unrecognized control: ${this.control}`)
    }
  }

  // We aren't trying to serialize any reads or writes. YOLO!
  execIfOps() {
    this.log('execIfOps')
    while (this.ops.length > 0) {
      assert((this.state === 'opened') || (this.state === 'draining'))
      const op = this.ops.shift()
      const [type, offset, sizeOrData, cb] = op
      switch (type) {
      case 'read':
        this.log('read')
        this.inFlightReads += 1
        const readSize = sizeOrData
        assert(readSize > 0)
        const readData = Buffer.alloc(readSize)
        FS.read(this.fd, readData, 0, readSize, offset, (err, read) => {
          this.inFlightReads -= 1
          if (err) {
            cb(err, null)
          } else if (!read) {
            cb(new Error('Could not satisfy length'), null)
          } else {
            if (read !== readSize) {
              log('read.short?', read, readSize, offset)
            }
            assert(read === readSize)
            cb(null, readData)
          }
          this.tick()
        })
        break
      case 'write':
        this.log('write')
        this.inFlightWrites += 1
        const writeData = sizeOrData
        FS.write(this.fd, writeData, 0, writeData.length, offset, (err, written) => {
          this.inFlightWrites -= 1
          if (err) {
            cb(err)
          } else {
            assert(written === writeData.length)
            cb(null)
          }
          this.tick()
        })
        break
      default:
        throw new Error(`Unrecognized type: ${type}`)
      }
    }
  }

  open() {
    this.log('open')
    assert(this.state === 'closed')
    this.state = 'opening'
    const _open = () => {
      FS.open(this.fullPath, READWRITE, (err, fd) => {
        if (err) { throw err }
        assert(this.state === 'opening')
        this.state = 'opened'
        this.fd = fd
        this.tick()
      })
    }
    if (this.fullDirMade) {
      _open()
    } else {
      mkdirp(this.fullDirPath, (err) => {
        if (err) { throw err }
        this.fullDirMade = true
        _open()
      })
    }
  }

  closeIfDrained() {
    this.log('closeIfDrained')
    assert((this.state === 'opened') || (this.state === 'draining'))
    if ((this.inFlightReads === 0) && (this.inFlightWrites === 0)) {
      this.state = 'closing'
      FS.close(this.fd, (err) => {
        if (err) { throw err }
        assert(this.state === 'closing')
        this.state = 'closed'
        this.fd = 0
        this.tick()
      })
    }
  }
}


class PathInterface {
  constructor(rafs, shortPath) {
    this.rafs = rafs
    this.shortPath = shortPath
  }

  read(offset, size, readCb) {
    this.rafs.op(this.shortPath, 'read', offset, size, readCb)
  }

  write(offset, data, writeCb) {
    this.rafs.op(this.shortPath, 'write', offset, data, writeCb)
  }
}


class RandomAccessFilesystem {
  constructor({ dirPath, maxOpenFiles }) {
    assert(dirPath)
    assert(maxOpenFiles)
    this.dirPath = dirPath
    this.maxOpenFiles = maxOpenFiles
    this.pathInterfaces = {}
    this.pathWorkers = {}
    this.numActivePathWorkers = 0
    this.stoppableQueue = new Deque(maxOpenFiles)
  }

  fullPath(shortPath) {
    return Path.join(this.dirPath, shortPath)
  }

  storageFor(shortPath) {
    if (!this.pathInterfaces[shortPath]) {
      assert(!this.pathWorkers[shortPath])
      const fullPath = this.fullPath(shortPath)
      this.pathInterfaces[shortPath] = new PathInterface(this, shortPath)
      this.pathWorkers[shortPath] = new PathWorker(fullPath)
    }
    return this.pathInterfaces[shortPath]
  }

  op(shortPath, type, offset, sizeOrData, cb) {
    const op = [type, offset, sizeOrData, cb]
    const pathWorker = this.pathWorkers[shortPath]
    assert(pathWorker)
    pathWorker.add(op)
    this.schedule(pathWorker)
  }

  schedule(pathWorker, backoff = 4) {
    // If the worker is already started, we're fine.
    if (pathWorker.control === 'start') {
      return
    }

    // If the worker is already stopping, we need to wait for it to finish
    // to allow it to properly cycle.
    assert(pathWorker.control === 'stop')
    if (pathWorker.state !== 'closed') {
      this.delay(pathWorker)
      return
    }

    assert(pathWorker.state === 'closed')
    // If there is file handle space available, use it to start the worker.
    if (this.numActivePathWorkers < this.maxOpenFiles) {
      this.numActivePathWorkers += 1
      pathWorker.start(() => {
        this.stoppableQueue.push(pathWorker)
      })
      return
    }

    // If there's a worker we can stop, do that to allow cycling. We won't be
    // able to do anything right away though, so wait.
    const stoppablePathWorker = this.stoppableQueue.shift()
    if (stoppablePathWorker) {
      stoppablePathWorker.stop(() => {
        this.numActivePathWorkers -= 1
      })
      this.delay(pathWorker)
      return
    }

    // Otherwise there is no fd space and all stopable workers are being
    // stopped, but not yet stopped, so wait a bit.
    this.delay(pathWorker)
  }

  delay(pathWorker) {
    setTimeout(() => this.schedule(pathWorker), 4)
  }
}

module.exports = RandomAccessFilesystem
