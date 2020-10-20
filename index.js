// Import stylesheets
import "./style.css";
// Firebase App (the core Firebase SDK) is always required and must be listed first
import * as firebase from "firebase/app";

// Add the Firebase products that you want to use
import "firebase/auth";
import "firebase/firestore";
import "firebase/analytics";

import * as firebaseui from "firebaseui";

// Document elements
const startRsvpButton = document.getElementById("startRsvp");
const guestbookContainer = document.getElementById("guestbook-container");
const labContainer= document.getElementById("lab-container");

const form = document.getElementById("leave-message");
const input = document.getElementById("message");
const guestbook = document.getElementById("guestbook");
const numberAttending = document.getElementById("number-attending");
const labTokenID = document.getElementById("lab-token-id");
const labLink = document.getElementById("lab-link");
const rsvpYes = document.getElementById("rsvp-yes");
const rsvpNo = document.getElementById("rsvp-no");

var rsvpListener = null;
var guestbookListener = null;


// Add Firebase project configuration object here
// Your web app's Firebase configuration
  var firebaseConfig = {
    apiKey: "AIzaSyDFSGc_OSlJMgP6RbTEbX3kuga7mZe_7QI",
    authDomain: "appdev-meetup-lab.firebaseapp.com",
    databaseURL: "https://appdev-meetup-lab.firebaseio.com",
    projectId: "appdev-meetup-lab",
    storageBucket: "appdev-meetup-lab.appspot.com",
    messagingSenderId: "726837527062",
    appId: "1:726837527062:web:cce33f2e35f92881016b37",
    measurementId: "G-KC51YMFQ95"
  };

 firebase.initializeApp(firebaseConfig);
 firebase.analytics();
labContainer.style.display = "none";
// FirebaseUI config
const uiConfig = {
  credentialHelper: firebaseui.auth.CredentialHelper.NONE,
  signInOptions: [
    // Email / Password Provider.
    firebase.auth.EmailAuthProvider.PROVIDER_ID
  ],
  callbacks: {
    signInSuccessWithAuthResult: function(authResult, redirectUrl) {
      // Handle sign-in.
      // Return false to avoid redirect.
      return false;
    }
  }
};

 const ui = new firebaseui.auth.AuthUI(firebase.auth());

 startRsvpButton.addEventListener("click",
 () => {
    if (firebase.auth().currentUser) {
      // User is signed in; allows user to sign out
      firebase.auth().signOut();
    } else {
      // No user is signed in; allows user to sign in
      ui.start("#firebaseui-auth-container", uiConfig);
    }
});

// Listen to the current Auth state
firebase.auth().onAuthStateChanged((user) => {
if (user){
  startRsvpButton.textContent = "LOGOUT";
  // Show guestbook to logged-in users
  guestbookContainer.style.display = "block";

  // Subscribe to the guestbook collection
  subscribeGuestbook();
  // Subscribe to the guestbook collection
  subscribeCurrentRSVP(user);
}
else{
  startRsvpButton.textContent = "LoGIN";
  // Hide guestbook for non-logged-in users
  guestbookContainer.style.display = "none";

  // Unsubscribe from the guestbook collection
  unsubscribeGuestbook();
  // Unsubscribe from the guestbook collection
  unsubscribeCurrentRSVP();

}
});

// Listen to the form submission
form.addEventListener("submit", (e) => {
 // Prevent the default form redirect
 e.preventDefault();
 // Write a new message to the database collection "guestbook"
 firebase.firestore().collection("guestbook").add({
   text: input.value,
   timestamp: Date.now(),
   name: firebase.auth().currentUser.displayName,
   userId: firebase.auth().currentUser.uid
 })
 // clear message input field
 input.value = ""; 
 // Return false to avoid redirect
 return false;
});

// Listen to guestbook updates
function subscribeGuestbook(){
   // Create query for messages
 guestbookListener = firebase.firestore().collection("guestbook")
 .orderBy("timestamp","desc")
 .onSnapshot((snaps) => {
   // Reset page
   guestbook.innerHTML = "";
   // Loop through documents in database
   snaps.forEach((doc) => {
     // Create an HTML entry for each document and add it to the chat
     const entry = document.createElement("p");
     entry.textContent = doc.data().name + ": " + doc.data().text;
     guestbook.appendChild(entry);
   });
 });
};

// Unsubscribe from guestbook updates
function unsubscribeGuestbook(){
 if (guestbookListener != null)
 {
   guestbookListener();
   guestbookListener = null;
 }
};

// Listen to RSVP responses
rsvpYes.onclick = () => {
 // Get a reference to the user's document in the attendees collection
 const userDoc = firebase.firestore().collection('attendees').doc(firebase.auth().currentUser.uid);

 // If they RSVP'd yes, save a document with attending: true
 userDoc.set({
   attending: true
 },{merge:true}).catch(console.error)
 labContainer.style.display = "block";
}
rsvpNo.onclick = () => {
 // Get a reference to the user's document in the attendees collection
 const userDoc = firebase.firestore().collection('attendees').doc(firebase.auth().currentUser.uid);

 // If they RSVP'd no, save a document with attending: false
 userDoc.set({
   attending: false
 }, {merge:true}).catch(console.error)
 labContainer.style.display = "none";
}

// Listen for attendee list
firebase.firestore()
.collection('attendees')
.where("attending", "==", true)
.onSnapshot(snap => {
 const newAttendeeCount = snap.docs.length;

 numberAttending.innerHTML = newAttendeeCount+' people taking the lab'; 
})

// Listen for attendee list
function subscribeCurrentRSVP(user){
 rsvpListener = firebase.firestore()
 .collection('attendees')
 .doc(user.uid)
 .onSnapshot((doc) => {
   if (doc && doc.data()){
     const attendingResponse = doc.data().attending;
     const token_id = doc.data().token_id;
     const lab_link = 'https://www.qwiklabs.com/focuses/12682?parent=catalog'

     // Update css classes for buttons
     if (attendingResponse){
       rsvpYes.className="clicked";
       rsvpNo.className="";
       
       if (token_id)
       {
         labTokenID.innerHTML = 'Qwicklab access token key: ' + token_id;
         //labLink.innerHTML = ' recommended lab is: ' + lab_link;
         console.log(token_id)
       }
       else
       {
         labTokenID.innerHTML = 'Token is not available, please contact meeting host';
         //labLink.innerHTML = ' recommended lab is: ' + lab_link;
         console.log('no token')
       }
       
     }
     else{
       rsvpYes.className="";
       rsvpNo.className="clicked";
       {
         labTokenID.innerHTML = '';   
       }
     }
   }
 });
}
function unsubscribeCurrentRSVP(){
 if (rsvpListener != null)
 {
   rsvpListener();
   rsvpListener = null;
 }
 rsvpYes.className="";
 rsvpNo.className="";
}
