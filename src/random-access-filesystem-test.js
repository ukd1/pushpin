const RAFS = require('./random-access-filesystem')
const assert = require('assert')

const rafs = new RAFS({ dirPath: '/Users/mmcgrana/Desktop/rafs-data', maxOpenFiles: 500 })


function getRandomInt(max) {
  return Math.floor(Math.random() * Math.floor(max));
}

function test(path, j) {
  setTimeout(() => {
    const raf = rafs.storageFor(path)
    raf.write(0, Buffer.from('hello '), (err) => {
      if (err) { throw err }
      raf.write(6, Buffer.from('world!'), (err) => {
        if (err) { throw err }
        raf.read(0, 12, (err, data) => {
          if (err) { throw err }
          console.log(`${path} (${j}): ${data.toString()}`)

          raf.write(0, Buffer.alloc(2048), (err) => {
            if (err) { throw err }
            raf.write(1024, Buffer.alloc(32768), (err) => {
              if (err) { throw err }
              let done = 0
              const onDone = () => {
                console.log(`${path} (${j}): hello blocks!`)
              }
              raf.read(0, 2048, (err, data) => {
                assert(data.length === 2048)
                if (err) { throw (err) }
                done += 1
                if (done == 2) { onDone() }
              })
              raf.read(1024, 32768, (err, data) => {
                assert(data.length === 32768)
                if (err) { throw (err) }
                done += 1
                if (done === 2) { onDone() }
              })
            })
          })
        })
      })
    })
  }, getRandomInt(10))
}

for (let j = 0; j < 1; j++) {
  for (let i = 0; i < 10000; i++) {
    const path = `foobarbat${i}`
    test(path, j)
  }
}
