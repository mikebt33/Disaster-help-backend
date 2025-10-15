import { sendPush } from "./fcmService.js";

const TEST_TOKEN = "dmLXTpBvQqGeCLH7eLy24t:APA91bG-ftOHJ_857HXjgWHBDy0RekEpj-X2vVhgmxa__Xhs68UC-8xUZt66wsdG7_XX4OUp7pYzxyP5GNfCKSYNTeSZeyiLXolg_PADsWBsvDKKWZ9C3SQ";

await sendPush(TEST_TOKEN, "🚨 Test Push", "Your backend FCM setup works perfectly!", {
  deeplink: "disasterhelp://detail?c=help-requests&id=test123",
});

process.exit(0);
