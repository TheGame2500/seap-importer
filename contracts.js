const mongo = require('mongodb')

async function getContracts() {
	const client = await mongo.MongoClient.connect('mongodb://localhost:3001/meteor')
	const db = client.db('meteor')

	const contracts = await db.collection('contracts')

	console.log('contracts count', await contracts.find().count())

	return contracts
}

module.exports = getContracts
