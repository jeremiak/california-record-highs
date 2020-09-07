const fs = require("fs")

const { max } = require("d3")
const glob = require("glob")
const { default: Queue } = require("p-queue")

const db = {}

glob("data/*.json", async (err, files) => {
  const queue = new Queue({ concurrency: 1 })
  files.forEach((file) => {
    queue.add(async () => {
      const buf = await fs.promises.readFile(file)
      const json = JSON.parse(buf.toString())
      const { id, lat, long, data } = json
      const tmax = data.filter((row) => row.element === "TMAX")
      let hottestTempAllTime = [null, null, null]
      let hottestTempSeptember = [null, null, null]
      let hottestTempToday = [null, null, null]

      const thisMonth = tmax.find((row) => {
        const { month, year } = row
        return month === "09" && year === "2020"
      })

      if (!thisMonth) return

      tmax.forEach((row) => {
        const { month, values, year } = row

        const hottest = max(values)

        if (!hottestTempAllTime[0]) {
          hottestTempAllTime[0] = hottest
          hottestTempAllTime[1] = month
          hottestTempAllTime[2] = year
        } else if (hottest > hottestTempAllTime[0]) {
          hottestTempAllTime[0] = hottest
          hottestTempAllTime[1] = month
          hottestTempAllTime[2] = year
        }

        if (month !== "09") return

        if (!hottestTempSeptember[0]) {
          hottestTempSeptember[0] = hottest
          hottestTempSeptember[1] = month
          hottestTempSeptember[2] = year
        } else if (hottest > hottestTempSeptember[0]) {
          hottestTempSeptember[0] = hottest
          hottestTempSeptember[1] = month
          hottestTempSeptember[2] = year
        }
      })

      db[id] = {
        lat,
        long,
        brokeAllTime:
          hottestTempAllTime[1] === "09" && hottestTempAllTime[2] === "2020",
        brokeSeptember:
          hottestTempSeptember[1] === "09" && hottestTempSeptember[2] === "2020",
        records: [hottestTempAllTime, hottestTempSeptember, hottestTempToday],
      }

      console.log(`Processed ${id}`)
    })
  })

  queue.onIdle().then(async () => {
    await fs.promises.writeFile('records.json', JSON.stringify(db, null, 2))
  })
})
