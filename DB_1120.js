const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs');
const csv = require('csv-parser');

// Database configuration
const DB_CONN_STRING = "mongodb://localhost:27017/";
const DB_NAME = "411630758";
const STDNTS_COLLECTION_NAME = "studentslist";

const client = new MongoClient(DB_CONN_STRING);

async function connectToDB() {
    await client.connect();
    console.log("Connected to MongoDB");
    return client.db(DB_NAME).collection(STDNTS_COLLECTION_NAME);
}

async function insertStudentsFromCSV(csvFilePath) {
    const collection = await connectToDB();
    const results = [];

    return new Promise((resolve, reject) => {
        fs.createReadStream(csvFilePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    const insertResult = await collection.insertMany(results);
                    console.log(`Inserted ${insertResult.insertedCount} students`);
                    resolve(insertResult);
                } catch (error) {
                    console.error("Error inserting data:", error);
                    reject(error);
                } finally {
                    await client.close();
                }
            });
    });
}

async function showAllStudents() {
    const collection = await connectToDB();
    const students = await collection.find().toArray();
    console.log("All students:", students);
    await client.close();
}

async function deleteDuplicate() {
    const collection = await connectToDB();
    const duplicates = await collection.aggregate([
        {
            $group: {
                _id: { 帳號: "$帳號", Email: "$Email" },
                count: { $sum: 1 },
                ids: { $push: "$_id" }
            }
        },
        { $match: { count: { $gt: 1 } } }
    ]).toArray();

    if (duplicates.length === 0) {
        console.log("No duplicates found.");
        await client.close();
        return;
    }

    let deletedCount = 0;
    for (const duplicate of duplicates) {
        const idsToDelete = duplicate.ids.slice(1);
        const deleteResult = await collection.deleteMany({ _id: { $in: idsToDelete } });
        deletedCount += deleteResult.deletedCount;
    }

    console.log(`Deleted ${deletedCount} duplicate documents.`);
    await client.close();
}

async function countByDepartment() {
    const collection = await connectToDB();
    const counts = await collection.aggregate([
        { $group: { _id: "$院系", count: { $sum: 1 } } }
    ]).toArray();
    console.log("Counts by department:", counts);
    await client.close();
}

async function addAbsencesField() {
    const collection = await connectToDB();
    const updateResult = await collection.updateMany(
        {},
        { $set: { 缺席次數: Math.floor(Math.random() * 6) } }
    );
    console.log(`Updated ${updateResult.modifiedCount} documents with random absences`);
    await client.close();
}

async function resetCollection() {
    const collection = await connectToDB();
    await collection.deleteMany({});
    console.log("Collection reset (all data deleted)");
    await client.close();
}

(async () => {
    try {
        // await insertStudentsFromCSV('studentslist.csv');
        await showAllStudents();
        // await deleteDuplicate();
        await countByDepartment();
        // await addAbsencesField();
        // await resetCollection();
    } catch (error) {
        console.error("An error occurred:", error);
    }
})();
