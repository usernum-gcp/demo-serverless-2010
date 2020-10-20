'use strict';

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const {Storage} = require('@google-cloud/storage');
const { Logging } = require('@google-cloud/logging');
const projectId = process.env.GCLOUD_PROJECT
const tokens_collection = '/labs/12-07-lab/qwicklab_tokens'
const path = require('path');
const os = require('os');
const fs = require('fs');


admin.initializeApp({
  credential: admin.credential.applicationDefault(),
  databaseURL: "https://appdev-meetup-lab.firebaseio.com"
});


/** When a user is created, create a qwicklab token for them */
exports.addLabTokenOnAuth = functions.auth.user().onCreate(async(user) => {
  console.log('createTokenOnCallTransaction started')
  try {
    const user_id = user.uid;
    const email = user.email; // The email of the user.
    const displayName = user.displayName; // The display name of the user.
    console.log('addLabTokenOnAuth started for user: %s, u_email: %s, uid: %s.', displayName, email, user_id)

    console.log('addLabTokenOnAuth: user ',user_id, 'token request')
    await admin.firestore().runTransaction(async(transaction) => {

      const usersRef = await admin.firestore().collection('attendees');
      const tokensRef = await admin.firestore()
          .collection(tokens_collection)
          .where("available", "==", true)
          .limit(1);

      // get next available token
      let tokenQ = await transaction.get(tokensRef);
      if (!tokenQ || tokenQ.size === 0)
      {
        console.log('addLabTokenOnAuth: no available tokens found')
        return;
      }


      const token_id = tokenQ.docs[0].data().token_id;
      const userDoc = await usersRef.doc(user_id);

      //update user record with assigned token
      await transaction.set(userDoc,
          {
            token_id: token_id,
            token_assigned: Date.now().toLocaleString(),
            email:email,
            displayName:displayName
          },{merge:true})
      console.log('addLabTokenOnAuth: user %s assigned %s on %s',user_id, token_id, Date.now().toLocaleString())
      //mark token as assigned
      await transaction.set(tokenQ.docs[0].ref,
          {available: false,
                assigned_on: Date.now().toLocaleString(),
                assigned_to: email}, {merge:true})
    })

  }catch (e) {
    console.log('addLabTokenOnAuth: something went wrong:', e)
  }
  console.log('addLabTokenOnAuth end')

  return;
});


exports.gcsFileSaved = functions.storage.object().onFinalize(async (object) => {
  console.log('gcsFileSaved: event received')
  try{
    const fileBucket = object.bucket; // The Storage bucket that contains the file.
    const filePath = object.name; // File path in the bucket.
    const fileName = path.basename(filePath); // Get the file name.
    const contentType = object.contentType; // File content type.

    // Download file from bucket.
    const bucket = admin.storage().bucket(fileBucket);
    const tempFilePath = path.join(os.tmpdir(), fileName);
    const metadata = {
      contentType: contentType,
    };
    await bucket.file(filePath).download({destination: tempFilePath});
    console.log('File %s downloaded locally to %s', fileName,  tempFilePath);

    const parse = require('csv-parser');
    let csv_parser = parse({delimiter: ','})
    fs.createReadStream(tempFilePath)
        .pipe(csv_parser)
        .on('data',async  function(data){
          try {
            const token_id = data["Token id"];

            const new_token = {
              available: true,
              token_id: token_id
            };
            // Add a new document in collection "cities" with ID 'LA'
            const res = await admin.firestore().collection(tokens_collection).add(new_token);

            console.log('createReadStream token_id %s added to collection %s with doc_id %s',
                token_id,
                tokens_collection,
               res.id)
          }
          catch(err) {
            console.log('gcsFileSaved:createReadStream: something went wrong:', err)
          }
        })
        .on('end',function(){
          console.log("createReadStream token_id saved")
        });

  }
  catch (e) {
    console.log('gcsFileSaved: something went wrong:', e)
  }
  console.log('gcsFileSaved end')
  return
});




