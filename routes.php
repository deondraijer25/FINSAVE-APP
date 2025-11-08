<?php

use Slim\App;
use Slim\Http\Request;
use Slim\Http\Response;

return function (App $app) {
    $container = $app->getContainer();

    # /\/\/\/\/\/\/\/\/\/\/\/  batches (group) API calls /\/\/\/\/\/\/\/\/\/\/\/\/\
    $app->group('/batches', function () use ($app, $container) {
        $app->get('/latest', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("batches latest", $args);
            $guid = $request->getQueryParam('guid');
            
            $body = $container->get('aqopidata')->latestDates($guid);
            return $response->withJson($body, 200);  
        });
        
        $app->post('/mbd', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("batches mbd", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $guid = $postValues['guid'];
            $userID = 1;
            $isPartner = $postValues['isPartner'];
            $belastingjaar = $postValues['belastingjaar'];
            
            $body = $container->get('aqopidata')->newMbd($userHash, $guid, $userID, $isPartner, $belastingjaar);
            return $response->withJson($body, 200);                
        });        
        
        $app->post('/uwv', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("batches uwv", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $guid = $postValues['guid'];
            $userID = 1;
            $isPartner = $postValues['isPartner'];
            
            $body = $container->get('aqopidata')->newUwv($userHash, $guid, $userID, $isPartner);
            return $response->withJson($body, 200);                
        });                
       
        $app->post('/moh', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("batches moh", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $guid = $postValues['guid'];
            $userID = 1;
            $isPartner = $postValues['isPartner'];
            
            $body = $container->get('aqopidata')->newMoh($userHash, $guid, $userID, $isPartner);
            return $response->withJson($body, 200);                
        });    
        
        $app->post('/mpo', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("batches mpo", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $guid = $postValues['guid'];
            $userID = 1;
            $isPartner = $postValues['isPartner'];
            $geslacht = $postValues['geslacht'];
            $naam = $postValues['naam'];
            
            $body = $container->get('aqopidata')->newMpo($userHash, $guid, $userID, $isPartner, $geslacht, $naam);
            return $response->withJson($body, 200);                
        });    
    });
    
    # /\/\/\/\/\/\/\/\/\/\/\/  data (group) API calls /\/\/\/\/\/\/\/\/\/\/\/\/\
    $app->group('/data', function () use ($app, $container) {
                
        $app->post('/mbd', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data mbd", $args);
            $postValues = $request->getParsedBody();
            
            $guid = $postValues['guid'];
            $userHash = $postValues['userHash'];
            $isPartner = $postValues['isPartner'];
            $data = $postValues['data'];
            
            $body = $container->get('mbd')->push($guid, $userHash, $isPartner, $data);
            return $response->withJson($body, 200);                
        });               
                
        $app->post('/uwv', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data uwv", $args);
            $postValues = $request->getParsedBody();
            
            $guid = $postValues['guid'];
            $userHash = $postValues['userHash'];
            $isPartner = $postValues['isPartner'];
            $data = $postValues['data'];
            
            $body = $container->get('uwv')->push($guid, $userHash, $isPartner, $data);
            return $response->withJson($body, 200);                
        });    
           
        $app->post('/moh', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data moh", $args);
            $postValues = $request->getParsedBody();
            
            $guid = $postValues['guid'];
            $userHash = $postValues['userHash'];
            $isPartner = $postValues['isPartner'];
            $data = $postValues['data'];
            
            $body = $container->get('moh')->push($guid, $userHash, $isPartner, $data);
            return $response->withJson($body, 200);                
        });    
        
        $app->post('/mpo', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data mpo", $args);
            $postValues = $request->getParsedBody();
            
            $guid = $postValues['guid'];
            $userHash = $postValues['userHash'];
            $isPartner = $postValues['isPartner'];
            $data = $postValues['data'];
            
            $body = $container->get('mpo')->push($guid, $userHash, $isPartner, $data);
            return $response->withJson($body, 200);                
        });    
        
        $app->post('/item', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data item", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $headerID = $postValues['headerID'];
            $tableName = $postValues['tableName'];
            $data = $postValues['data'];
            
            $body = $container->get('aqopidata')->addRecordItem($userHash, $headerID, $tableName, $data);
            return $response->withJson($body, 200);                
        });  
        
        $app->post('/items', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data items", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $headerID = $postValues['headerID'];
            $tableName = $postValues['tableName'];
            $data = $postValues['data'];
            
            $body = $container->get('aqopidata')->addRecordList($userHash, $headerID, $tableName, $data);
            return $response->withJson($body, 200);      
        });         
        
        $app->post('/child', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data child", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $headerID = $postValues['headerID'];
            $tableName = $postValues['tableName'];
            $data = $postValues['data'];
            $parentColumn = $postValues['parentColumn'];
            $parentValue = $postValues['parentValue'];
            
            $body = $container->get('aqopidata')->addChildRecordItem($userHash, $headerID, $tableName, $data, $parentColumn, $parentValue);
            return $response->withJson($body, 200);   
        });  
        
        $app->post('/children', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data children", $args);
            $postValues = $request->getParsedBody();
            
            $userHash = $postValues['userHash'];
            $headerID = $postValues['headerID'];
            $tableName = $postValues['tableName'];
            $data = $postValues['data'];
            $parentColumn = $postValues['parentColumn'];
            $parentValue = $postValues['parentValue'];
            
            $body = $container->get('aqopidata')->addChildRecordList($userHash, $headerID, $tableName, $data, $parentColumn, $parentValue);
            return $response->withJson($body, 200); 
        });      
        
        $app->post('/scrapings', function (Request $request, Response $response, array $args) use ($container) {
            /*
            // all above routes (in data group) are now deprecated. this is only route to be used in data group
            */
            $container->get('logger')->info("data scrapings", $args);
            $postValues = $request->getParsedBody();            
            $guid = $postValues['guid'];
            $firebase_uid = $request->getHeaderLine('fb_uid');
            $firebase_email = $request->getHeaderLine('fb_email');
            $isPartner = $postValues['isPartner'];
            $data = $postValues['data'];            
            $body = $container->get('scrapings')->push($guid, $firebase_uid, $firebase_email, $isPartner, $data);
            return $response->withJson($body, 200);                
        });         
        
        $app->get('/scrapings/latest', function (Request $request, Response $response, array $args) use ($container) {
            $container->get('logger')->info("data scrapings latest", $args);
            $guid = $request->getQueryParam('guid');            
            $body = $container->get('scrapings')->latest($guid);
            return $response->withJson($body, 200);             
        }); 
                
    });

};
