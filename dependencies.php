<?php

use Slim\App;

require_once( __DIR__.'/api/services/AqopiDataService.php');
require_once( __DIR__.'/api/services/MbdService.php');
require_once( __DIR__.'/api/services/UwvService.php');
require_once( __DIR__.'/api/services/MohService.php');
require_once( __DIR__.'/api/services/MpoService.php');
require_once( __DIR__.'/api/services/ScrapingsService.php');

return function (App $app) {
    $container = $app->getContainer();

    // view renderer
    $container['renderer'] = function ($c) {
        $settings = $c->get('settings')['renderer'];
        return new \Slim\Views\PhpRenderer($settings['template_path']);
    };

    // monolog
    $container['logger'] = function ($c) {
        $settings = $c->get('settings')['logger'];
        $logger = new \Monolog\Logger($settings['name']);
        $logger->pushProcessor(new \Monolog\Processor\UidProcessor());
        $logger->pushHandler(new \Monolog\Handler\StreamHandler($settings['path'], $settings['level']));
        return $logger;
    };

    // database
    $container['db'] = function ($c) {
        $s = $c->get('settings')['db'];
        try {
            $conn = new mysqli($s['host'], $s['user'], $s['pass'], $s['database'], $s['port']);
        } catch (Exception $e ) {
          echo "Service unavailable[".$e->message."]";
        }    
        $conn->set_charset("utf8");
        return $conn;
    };

    // aqopidata service
    $container['aqopidata'] = function ($container) {
        return new AqopiDataService($container['db']);
    };

    // mbd service
    $container['mbd'] = function ($container) {
        return new MbdService($container['db']);
    };
    
    // uwv service
    $container['uwv'] = function ($container) {
        return new UwvService($container['db']);
    };    
    
    // moh service
    $container['moh'] = function ($container) {
        return new MohService($container['db']);
    };    
    
    // mpo service
    $container['mpo'] = function ($container) {
        return new MpoService($container['db']);
    };    

    // scrapings service
    $container['scrapings'] = function ($container) {
        return new ScrapingsService($container['db']);
    };    
        
    
};
