//
//  OnLoad.mm
//  WatermelonDB
//
//  Created by BuildOpsLA27 on 7/16/25.
//  Copyright © 2025 Nozbe. All rights reserved.
//

#import "JSISwiftWrapperModule.h"
#import <Foundation/Foundation.h>
#import <ReactCommon/CxxTurboModuleUtils.h>

@interface OnLoad: NSObject
@end

@implementation OnLoad

+ (void) load {
    facebook::react::registerCxxModuleToGlobalModuleMap(
                                                        std::string(facebook::react::JSISwiftWrapperModule::kModuleName),
                                                        [](std::shared_ptr<facebook::react::CallInvoker> jsInvoker) {
                                                            return std::make_shared<facebook::react::JSISwiftWrapperModule>(jsInvoker);
                                                        }
                                                        );
}

@end
