
// Copyright 2009-2015 David Thorpe
// https://github.com/djthorpe/postgresql-kit
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

#import <PGControlsKit/PGControlsKit.h>
#import "PGSourceViewConnection.h"

@implementation PGSourceViewConnection

////////////////////////////////////////////////////////////////////////////////
// properties

@synthesize iconStatus;

////////////////////////////////////////////////////////////////////////////////
// private methods

-(NSImage* )imageForStatus:(PGSourceViewConnectionIcon)status {
	NSBundle* thisBundle = [NSBundle bundleForClass:[self class]];
	NSParameterAssert(thisBundle);
	switch(status) {
	case PGSourceViewConnectionIconDisconnected:
		return [thisBundle imageForResource:@"traffic-grey"];
	case PGSourceViewConnectionIconConnecting:
		return [thisBundle imageForResource:@"traffic-orange"];
	case PGSourceViewConnectionIconConnected:
		return [thisBundle imageForResource:@"traffic-green"];
	case PGSourceViewConnectionIconRejected:
		return [thisBundle imageForResource:@"traffic-red"];
	}
}

////////////////////////////////////////////////////////////////////////////////
// overrides

-(BOOL)isGroupItem {
	return NO;
}

-(BOOL)shouldSelectItem {
	return YES;
}

-(NSTableCellView* )cellViewForOutlineView:(NSOutlineView* )outlineView tableColumn:(NSTableColumn* )tableColumn owner:(id)owner {
	NSTableCellView* cellView = [super cellViewForOutlineView:outlineView tableColumn:tableColumn owner:owner];
	NSParameterAssert(cellView);
	
	NSImage* trafficIcon = [self imageForStatus:PGSourceViewConnectionIconDisconnected];
	[[cellView imageView] setImage:trafficIcon];
	
	return cellView;
}

@end