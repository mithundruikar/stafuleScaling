package org.geode.scaling.app.cluster;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;

public class GeodeMembershipListener implements MembershipListener {
    private static Logger LOGGER = LoggerFactory.getLogger(GeodeMembershipListener.class);

    private Cache cache;
    private PartitionOwnershipRegion partitionOwnershipRegion;
    public GeodeMembershipListener(Cache cache, PartitionOwnershipRegion partitionOwnershipRegion) {
        this.cache = cache;
        this.partitionOwnershipRegion = partitionOwnershipRegion;
    }

    @PostConstruct
    public void init() {
        ManagementService.getExistingManagementService(cache).addMembershipListener(this);
    }

    @Override
    public void memberJoined(MembershipEvent membershipEvent) {
        LOGGER.info(String.format("Member joined %s", membershipEvent.getDistributedMember().getName()));
        this.partitionOwnershipRegion.addPartitionId(membershipEvent.getDistributedMember().getName());
    }

    @Override
    public void memberLeft(MembershipEvent membershipEvent) {
        LOGGER.info(String.format("Member departed left id %s", membershipEvent.getDistributedMember().getName()));
        this.partitionOwnershipRegion.removePartitionId(membershipEvent.getDistributedMember().getName());
    }

    @Override
    public void memberCrashed(MembershipEvent membershipEvent) {
        LOGGER.info(String.format("Member departed crashed id %s", membershipEvent.getDistributedMember().getName()));
        this.partitionOwnershipRegion.removePartitionId(membershipEvent.getDistributedMember().getName());
    }

}
