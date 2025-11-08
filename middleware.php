<?php

use Slim\App;
require_once('firebase.php');

return function (App $app) {
    $app->add( new FirebaseMiddleware() );
};